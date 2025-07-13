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

	// 将此回调作为每日文件切割的定时检查器
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

		// 1. 检查是否跨天，如果跨天则执行轮转，并跳过本次周期性上传
		if now.Year() > record.StartTime.Year() || now.YearDay() > record.StartTime.YearDay() {
			log.Printf("Daily rotation triggered for stream %s. Kicking session to rotate file.", group.StreamName)

			// 调用lalserver的HTTP API来踢掉会话，这将触发OnPubStop
			kickPayload := base.ApiCtrlKickSessionReq{
				StreamName: group.StreamName,
				SessionId:  group.StatPub.SessionId,
			}
			h.sm.CtrlKickSession(kickPayload)
			// 踢出后，由OnPubStop处理，此处跳过
			continue
		}

		// 2. 如果未跨天，执行周期性上传（异步）
		originalPath := record.VideoPath
		if _, err := os.Stat(originalPath); os.IsNotExist(err) {
			log.Printf("Recording file %s does not exist yet for periodic upload.", originalPath)
			continue
		}

		log.Printf("Periodic upload triggered for stream %s.", group.StreamName)
		go func(path, stream string) {
			objectName, err := uploadFileToOSS(path, stream)
			if err != nil {
				log.Printf("Failed to perform periodic upload for stream %s: %v", stream, err)
			} else {
				log.Printf("Periodic upload successful for stream %s to object %s", stream, objectName)
			}
		}(originalPath, group.StreamName)
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
		log.Printf("Could not find recording record for stream %s to stop: %v", info.StreamName, result.Error)
		return
	}

	// 1. 重命名文件，加入日期和时间以确保唯一性
	originalPath := record.VideoPath
	// 使用 "2006-01-02-15-04-05" 格式确保文件名在每天的多次推流中是唯一的
	dailyFileName := fmt.Sprintf("%s-%s.flv", info.StreamName, record.StartTime.Format("2006-01-02-15-04-05"))
	newPath := filepath.Join(filepath.Dir(originalPath), dailyFileName)

	if _, err := os.Stat(originalPath); os.IsNotExist(err) {
		log.Printf("Recording file %s not found, it might have been processed by a concurrent OnPubStop. Stream: %s", originalPath, info.StreamName)
		// 文件不存在，可能已经被处理，或者lalserver没有成功创建文件。
		// 尝试将记录标记为错误状态，以便排查。
		db.Model(&record).Update("status", "finished_file_not_found")
		return
	}

	if err := os.Rename(originalPath, newPath); err != nil {
		log.Printf("Failed to rename file from %s to %s: %v", originalPath, newPath, err)
		// 即使重命名失败，也尝试更新数据库状态
		db.Model(&record).Update("status", "finished_rename_failed")
		return
	}
	log.Printf("Renamed file to %s", newPath)

	// 2. 上传重命名后的文件到 OSS
	objectName, err := uploadFileToOSS(newPath, info.StreamName)
	if err != nil {
		log.Printf("Failed to upload FLV file to OSS: %v", err)
		// 上传失败，更新状态以供后续处理
		db.Model(&record).Update("status", "finished_upload_failed")
		return
	}

	// 3. 更新数据库记录
	ossURL := fmt.Sprintf("https://leep-oss.oss-cn-shanghai.aliyuncs.com/%s", objectName)
	updates := CameraRecord{
		Status:   "finished",
		VideoURL: ossURL,
		// 更新 VideoPath 为重命名后的路径
		VideoPath: newPath,
	}

	result = db.Model(&record).Updates(updates)
	if result.Error != nil {
		log.Printf("Failed to update record for stream %s: %v", info.StreamName, result.Error)
		return
	}

	log.Printf("Stream %s marked as finished, file uploaded to %s", info.StreamName, ossURL)

	// 4. 归档成功后，删除本地文件
	if err := os.Remove(newPath); err != nil {
		log.Printf("Failed to remove local file %s after archiving: %v", newPath, err)
	} else {
		log.Printf("Removed local file %s.", newPath)
	}
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
