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
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StreamName string `gorm:"index"`
	VideoPath  string
	StartTime  time.Time `gorm:"index"`
	Status     string
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

	taskQueue chan PostTask
	client    *http.Client
}

func NewHttpNotify(cfg HttpNotifyConfig, serverId string) *HttpNotify {
	// 初始化数据库连接
	initDB()

	httpNotify := &HttpNotify{
		cfg:       cfg,
		serverId:  serverId,
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
	h.NotifyUpdate(info)
}

func (h *HttpNotify) OnPubStart(info base.PubStartInfo) {
	// h.NotifyPubStart(info)

	// 插入新的记录
	startTime := time.Now()
	record := CameraRecord{
		StreamName: info.StreamName,
		VideoPath:  filepath.Join("lal_record/flv/", fmt.Sprintf("%s.flv", info.StreamName)),
		StartTime:  startTime,
		Status:     "recording",
	}

	result := db.Create(&record)
	if result.Error != nil {
		log.Printf("Failed to insert record into database: %v", result.Error)
		return
	}

	log.Printf("Inserted new record for stream: %s", info.StreamName)
}

func (h *HttpNotify) OnPubStop(info base.PubStopInfo) {
	// h.NotifyPubStop(info)

	// 更新对应 stream 的记录状态为 finished
	result := db.Model(&CameraRecord{}).
		Where("stream_name = ? AND status = ?", info.StreamName, "recording").
		Update("status", "finished")
	if result.Error != nil {
		log.Printf("Failed to update record for stream %s: %v", info.StreamName, result.Error)
		return
	}

	log.Printf("Stream %s marked as finished", info.StreamName)
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
	// h.NotifyOnHlsMakeTs(info)

	// Log the event
	log.Printf("HLS TS file created: %s", info.TsFile)

	// Construct the FLV file path (assuming the FLV file is in the same directory as the TS file)
	flvFilePath := filepath.Join("lal_record/flv/", fmt.Sprintf("%s.flv", info.StreamName))

	// Upload the FLV file to OSS
	err := uploadFileToOSS(flvFilePath, info.StreamName)
	if err != nil {
		log.Printf("Failed to upload FLV file to OSS: %v", err)
	}
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
func uploadFileToOSS(filePath, streamName string) error {
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
		return err
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
		return err
	}
	log.Printf("Successfully uploaded file to OSS: %s to object %s", file.Name(), objectName)
	return nil
}
