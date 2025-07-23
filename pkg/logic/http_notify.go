// Copyright 2020, Chef.  All rights reserved.
// https://github.com/q191201771/lal
//
// Use of this source code is governed by a MIT-style license
// that can be found in the License file.
//
// Author: Chef (191201771@qq.com)

package logic

import (
	"net/http"
	"time"

	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/naza/pkg/nazahttp"

	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// CameraRecord 摄像头记录表结构
type CameraRecord struct {
	ID         int64     `json:"id" db:"id"`                             // 主键ID
	CreatedAt  time.Time `json:"created_at,omitempty" db:"created_at"`   // 创建时间，允许为NULL
	UpdatedAt  time.Time `json:"updated_at,omitempty" db:"updated_at"`   // 更新时间，允许为NULL
	StreamName string    `json:"stream_name,omitempty" db:"stream_name"` // 流名称，允许为NULL
	VideoURL   string    `json:"video_url,omitempty" db:"video_url"`     // 视频URL，允许为NULL
	StartTime  time.Time `json:"start_time,omitempty" db:"start_time"`   // 开始时间，允许为NULL
	Status     string    `json:"status,omitempty" db:"status"`           // 状态，允许为NULL
	VideoPath  string    `json:"video_path,omitempty" db:"video_path"`   // 视频路径，允许为NULL
}

func (CameraRecord) TableName() string {
	return "camera_records"
}

var db *gorm.DB

// 初始化数据库连接
func initDB() {
	dsn := "host=139.196.177.102 user=leep_user password=Qiuji2024! dbname=leep port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 自动迁移（创建表）
	db.AutoMigrate(&CameraRecord{})
	log.Println("Database connected and migrated successfully")
}

// TODO(chef): refactor 配置参数供外部传入
// TODO(chef): refactor maxTaskLen修改为能表示是阻塞任务的意思
var (
	maxTaskLen       = 1024
	notifyTimeoutSec = 3
)

type PostTask struct {
	url  string
	info interface{}
}

type HttpNotify struct {
	cfg HttpNotifyConfig

	serverId string
	sm       *ServerManager

	taskQueue chan PostTask
	client    *http.Client
}

func NewHttpNotify(cfg HttpNotifyConfig, serverId string, sm *ServerManager) *HttpNotify {
	// 初始化数据库连接
	initDB()

	httpNotify := &HttpNotify{
		cfg:       cfg,
		serverId:  serverId,
		sm:        sm,
		taskQueue: make(chan PostTask, maxTaskLen),
		client: &http.Client{
			Timeout: time.Duration(notifyTimeoutSec) * time.Second,
		},
	}
	go httpNotify.RunLoop()

	return httpNotify
}

// TODO(chef): Dispose

// ---------------------------------------------------------------------------------------------------------------------

func (h *HttpNotify) NotifyServerStart(info base.LalInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnServerStart, info)
}

func (h *HttpNotify) NotifyUpdate(info base.UpdateInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnUpdate, info)
}

func (h *HttpNotify) NotifyPubStart(info base.PubStartInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnPubStart, info)
}

func (h *HttpNotify) NotifyPubStop(info base.PubStopInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnPubStop, info)
}

func (h *HttpNotify) NotifySubStart(info base.SubStartInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnSubStart, info)
}

func (h *HttpNotify) NotifySubStop(info base.SubStopInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnSubStop, info)
}

func (h *HttpNotify) NotifyPullStart(info base.PullStartInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnRelayPullStart, info)
}

func (h *HttpNotify) NotifyPullStop(info base.PullStopInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnRelayPullStop, info)
}

func (h *HttpNotify) NotifyRtmpConnect(info base.RtmpConnectInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnRtmpConnect, info)
}

func (h *HttpNotify) NotifyOnHlsMakeTs(info base.HlsMakeTsInfo) {
	info.ServerId = h.serverId
	h.asyncPost(h.cfg.OnHlsMakeTs, info)
}

// ----- implement INotifyHandler interface ----------------------------------------------------------------------------

func (h *HttpNotify) OnServerStart(info base.LalInfo) {
	h.NotifyServerStart(info)
}

func (h *HttpNotify) OnUpdate(info base.UpdateInfo) {
    // h.NotifyUpdate(info)

    // 每次更新时检查所有正在推流的流，处理以下场景:
    // 1. 检查是否录制时长已超过6小时，如果超过则进行视频轮转
    // 2. 检查是否跨天，如果跨天则执行轮转
    // 3. 检查是否跨越10分钟间隔，如果是则执行轮转，避免文件过大
    now := time.Now()
    for _, group := range info.Groups {
        // 只关心正在推的流
        if group.StatPub.SessionId == "" {
            continue
        }

        // 从数据库查找对应的"recording"记录
        var record CameraRecord
        result := db.Where("stream_name = ? AND status = ?", group.StreamName, "recording").First(&record)
        if result.Error != nil {
            // 找不到记录，可能是在OnPubStart之前触发了，忽略
            continue
        }

        // 计算当前录制时长
        recordDuration := now.Sub(record.StartTime)
        sixHours := 6 * time.Hour

        // 1. 检查是否超过6小时，如果超过则执行轮转
        if recordDuration >= sixHours {
            log.Printf("流 %s 的录制已超过6小时 (%s)。正在轮转视频文件。",
                group.StreamName, recordDuration.String())

            // 调用lalserver的HTTP API来踢掉会话，这将触发OnPubStop
            kickPayload := base.ApiCtrlKickSessionReq{
                StreamName: group.StreamName,
                SessionId:  group.StatPub.SessionId,
            }
            h.sm.CtrlKickSession(kickPayload)
            // 踢出后，由OnPubStop处理，此处跳过继续检查其他流
            continue
        }

        // 2. 检查是否跨天，如果跨天则执行轮转
        if now.Year() > record.StartTime.Year() || now.YearDay() > record.StartTime.YearDay() {
            log.Printf("触发了流 %s 的每日轮转。踢出会话以轮转文件。",
                group.StreamName)

            // 调用lalserver的HTTP API来踢掉会话，这将触发OnPubStop
            kickPayload := base.ApiCtrlKickSessionReq{
                StreamName: group.StreamName,
                SessionId:  group.StatPub.SessionId,
            }
            h.sm.CtrlKickSession(kickPayload)
            // 踢出后，由OnPubStop处理，此处跳过
            continue
        }

        // 3. 检查是否跨越10分钟间隔，如果是则执行轮转
        // 原来的1小时轮转逻辑（注释保留）：
        // if recordDuration.Minutes() > 1 && record.StartTime.Hour() != now.Hour() {
        //     log.Printf("流 %s 录制已经跨越整点小时 (当前: %d时，开始: %d时)。执行每小时文件轮转。",
        //         group.StreamName, now.Hour(), record.StartTime.Hour())
        //
        //     kickPayload := base.ApiCtrlKickSessionReq{
        //         StreamName: group.StreamName,
        //         SessionId:  group.StatPub.SessionId,
        //     }
        //     h.sm.CtrlKickSession(kickPayload)
        //     continue
        // }

        // 新的10分钟轮转逻辑:
        // 计算开始时间的10分钟区间和当前时间的10分钟区间
        startInterval := record.StartTime.Minute() / 10
        currentInterval := now.Minute() / 10
        
        // 如果跨越了小时或10分钟间隔，并且录制时长至少超过1分钟（避免频繁轮转）
        if recordDuration.Minutes() > 1 && (record.StartTime.Hour() != now.Hour() || startInterval != currentInterval) {
            log.Printf("流 %s 录制已经跨越10分钟间隔 (当前: %d时%d分，开始: %d时%d分)。执行10分钟文件轮转。",
                group.StreamName, now.Hour(), now.Minute(), record.StartTime.Hour(), record.StartTime.Minute())

            kickPayload := base.ApiCtrlKickSessionReq{
                StreamName: group.StreamName,
                SessionId:  group.StatPub.SessionId,
            }
            h.sm.CtrlKickSession(kickPayload)
            continue
        }
    }
}

func (h *HttpNotify) OnPubStart(info base.PubStartInfo) {
	// h.NotifyPubStart(info)

	// 推流开始时，只记录一个初始状态
	// 检查是否已有未完成的记录，避免重复插入
	var record CameraRecord
	result := db.Where("stream_name = ? AND status = ?", info.StreamName, "recording").First(&record)
	if result.Error == nil {
		log.Printf("Stream %s is already marked as recording. Skipping new record creation.", info.StreamName)
		return
	}

	// 插入新的记录
	startTime := time.Now()
	record = CameraRecord{
		StreamName: info.StreamName,
		// VideoPath 存储 lalserver 默认的录制路径
		VideoPath: filepath.Join("lal_record/flv/", fmt.Sprintf("%s.flv", info.StreamName)),
		StartTime: startTime,
		Status:    "recording",
		// VideoURL 此时为空，在推流结束后更新
		VideoURL: "",
	}

	result = db.Create(&record)
	if result.Error != nil {
		log.Printf("Failed to insert record into database: %v", result.Error)
		return
	}

	log.Printf("Inserted new record for stream: %s", info.StreamName)

}

func (h *HttpNotify) OnPubStop(info base.PubStopInfo) {
	// h.NotifyPubStop(info)

	// 查找对应的 "recording" 记录
	var record CameraRecord
	result := db.Where("stream_name = ? AND status = ?", info.StreamName, "recording").First(&record)
	if result.Error != nil {
		log.Printf("无法找到流 %s 的录制记录以停止: %v", info.StreamName, result.Error)
		return
	}

	// 1. 重命名文件，加入日期和时间以确保唯一性
	originalPath := record.VideoPath
	// 使用 "2006-01-02-15-04-05" 格式确保文件名在每天的多次推流中是唯一的
	dailyFileName := fmt.Sprintf("%s-%s.flv", info.StreamName, record.StartTime.Format("2006-01-02-15-04-05"))
	newPath := filepath.Join(filepath.Dir(originalPath), dailyFileName)

	if _, err := os.Stat(originalPath); os.IsNotExist(err) {
		log.Printf("未找到录制文件 %s，可能已被并发的OnPubStop处理。流名称: %s", originalPath, info.StreamName)
		// 文件不存在，可能已经被处理，或者lalserver没有成功创建文件。
		// 尝试将记录标记为错误状态，以便排查。
		db.Model(&record).Update("status", "finished_file_not_found")
		return
	}

	if err := os.Rename(originalPath, newPath); err != nil {
		log.Printf("重命名文件失败，从 %s 到 %s: %v", originalPath, newPath, err)
		// 即使重命名失败，也尝试更新数据库状态
		db.Model(&record).Update("status", "finished_rename_failed")
		return
	}
	log.Printf("文件已重命名为 %s", newPath)

	// 2. 直接更新数据库记录，保留本地文件路径
	updates := CameraRecord{
		Status:    "finished",
		VideoPath: newPath, // 更新为重命名后的本地路径
	}

	result = db.Model(&record).Updates(updates)
	if result.Error != nil {
		log.Printf("更新流 %s 的记录失败: %v", info.StreamName, result.Error)
		return
	}

	log.Printf("流 %s 已标记为完成，本地文件保存在 %s", info.StreamName, newPath)
}

func (h *HttpNotify) OnSubStart(info base.SubStartInfo) {
	h.NotifySubStart(info)
}

func (h *HttpNotify) OnSubStop(info base.SubStopInfo) {
	h.NotifySubStop(info)
}

func (h *HttpNotify) OnRelayPullStart(info base.PullStartInfo) {
	h.NotifyPullStart(info)
}

func (h *HttpNotify) OnRelayPullStop(info base.PullStopInfo) {
	h.NotifyPullStop(info)
}

func (h *HttpNotify) OnRtmpConnect(info base.RtmpConnectInfo) {
	h.NotifyRtmpConnect(info)
}

func (h *HttpNotify) OnHlsMakeTs(info base.HlsMakeTsInfo) {
	h.NotifyOnHlsMakeTs(info)
}

// ---------------------------------------------------------------------------------------------------------------------

func (h *HttpNotify) RunLoop() {
	for {
		select {
		case t := <-h.taskQueue:
			h.post(t.url, t.info)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func (h *HttpNotify) asyncPost(url string, info interface{}) {
	if !h.cfg.Enable || url == "" {
		return
	}

	select {
	case h.taskQueue <- PostTask{url: url, info: info}:
		// noop
	default:
		Log.Error("http notify queue full.")
	}
}

func (h *HttpNotify) post(url string, info interface{}) {
	if _, err := nazahttp.PostJson(url, info, h.client); err != nil {
		Log.Errorf("http notify post error. err=%+v, url=%s, info=%+v", err, url, info)
	}
}

// uploadFileToOSS 上传文件到OSS
func uploadFileToOSS(filePath, streamName string) (string, error) {
	// 创建OSS客户端
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion("cn-shanghai")

	client := oss.NewClient(cfg)

	// 获取存储空间
	bucketName := "leep-oss"

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return "", err
	}
	defer file.Close()

	// Get the current date in the format YYYY-MM-DD
	currentDate := time.Now().Format("2006-01-02")

	// Construct the object name with streamname, appname, date, and filename
	objectName := fmt.Sprintf("%s/%s/%s", streamName, currentDate, filepath.Base(file.Name()))

	_, err = client.PutObject(context.TODO(), &oss.PutObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
		Body:   file,
	})

	if err != nil {
		log.Printf("Error uploading file to OSS: %v", err)
		return "", err
	}
	log.Printf("Successfully uploaded file to OSS: %s to object %s", file.Name(), objectName)
	return objectName, nil
}
