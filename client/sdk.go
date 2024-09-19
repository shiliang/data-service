/*
	@author: shiliang
	@date: 2024/9/6
	@note: 客户端sdk，供外部服务使用

*
*/
package client

import (
	"context"
	pb "data-service/generated/datasource"
	"data-service/utils"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"log"
	"net/http"
	"os"
)

type DataServiceClient struct {
	client pb.DataSourceServiceClient
	conn   *grpc.ClientConn
	logger *zap.SugaredLogger
	ctx    context.Context
}

/***
  * @Description 创建数据服务组件sdk实例
  * @return DataServiceClient 数据服务客户端
  *
***/
func NewDataServiceClient(ctx context.Context, serverInfo *pb.ServerInfo) *DataServiceClient {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	// 将 ServerInfo 存放到 context 中
	ctx = context.WithValue(ctx, "serverInfo", serverInfo)
	// 创建data service客户端
	return &DataServiceClient{
		logger: sugar,
		ctx:    ctx,
	}
}

func (sdk *DataServiceClient) SetClient(client pb.DataSourceServiceClient) {
	sdk.client = client
}

/**
 * @Description 从服务端读取apache arrow的数据
 * @Param
 * @return
 **/
func (sdk *DataServiceClient) ReadBatchData(ctx context.Context, request *pb.BatchReadRequest) (*pb.Response, error) {
	err := sdk.connect(ctx)
	if err != nil {
		return nil, err
	}
	wrappedRequest := &pb.WrappedReadRequest{
		Request:   request,
		RequestId: uuid.New().String(),
	}
	// 调用服务端ReadData方法
	response, err := sdk.client.ReadBatchData(ctx, wrappedRequest)
	if err == nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	if err := sdk.close(); err != nil {
		return nil, fmt.Errorf("failed to close grpc server: %w", err)
	}
	// 读取的数据文件写入到minio中，需要下载到本地
	err = DownloadFile(response.GetObjectUrl(), request.GetFilePath())
	if err != nil {
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("Failed to read data: %v", err),
		}, nil
	}
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("success to read data"),
	}, nil
}

/**
 * @Description 流式任务读取数据（小数据量）
 * @Param request 读取请求，包含数据源信息
 * @return
 **/
func (sdk *DataServiceClient) ReadStreamingData(ctx context.Context, request *pb.StreamReadRequest) (*pb.Response, error) {
	err := sdk.connect(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := sdk.client.ReadStreamingData(ctx, request)
	if err != nil {
		sdk.logger.Warnw("Failed to read data", "error", err)
		return nil, fmt.Errorf("error calling ReadStreamingData: %w", err)
	}
	if err := sdk.close(); err != nil {
		return nil, fmt.Errorf("failed to close grpc server: %w", err)
	}
	filePath := request.FilePath
	switch request.FileType {
	case pb.FileType_FILE_TYPE_CSV:
		err := utils.ConvertDataToFile(stream, filePath, sdk.logger, pb.FileType_FILE_TYPE_CSV)
		if err != nil {
			return nil, fmt.Errorf("error converting data to CSV: %w", err)
		}
	case pb.FileType_FILE_TYPE_ARROW:
		err := utils.ConvertDataToFile(stream, filePath, sdk.logger, pb.FileType_FILE_TYPE_ARROW)
		if err != nil {
			return nil, fmt.Errorf("error converting data to arrow: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported file type: %v", request.FileType)
	}
	return &pb.Response{Success: true}, nil
}

// 建立grpc连接
func (client *DataServiceClient) connect(ctx context.Context) error {
	serverInfo, ok := ctx.Value("serverInfo").(pb.ServerInfo)
	if !ok {
		return errors.New("failed to retrieve ServerInfo from context")
	}
	var serverAddress string
	if serverInfo.GetNamespace() == "" {
		serverAddress = fmt.Sprintf("%s:%s", serverInfo.GetServiceName(), serverInfo.GetServicePort())
	} else {
		serverAddress = fmt.Sprintf("%s.%s.svc.cluster.local:%s", serverInfo.GetServiceName(),
			serverInfo.GetNamespace(), serverInfo.GetServicePort())
	}
	// 建立grpc连接
	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		// 连接失败
		return fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client.client = pb.NewDataSourceServiceClient(conn)
	client.conn = conn
	return nil
}

// 关闭gRPC连接
func (client *DataServiceClient) close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("failed to disconnect to gRPC server: %v", err)
	}
	return nil
}

// 写入数据文件到OSS，文件名为requestId + taskId
func (sdk *DataServiceClient) WriteOSSData(ctx context.Context, request *pb.OSSWriteRequest) (*pb.Response, error) {
	err := sdk.connect(ctx)
	if err != nil {
		return nil, err
	}
	response, err := sdk.client.WriteOSSData(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// 把本地文件写入minio，写入文件名根据requestId和dataName
func (client *DataServiceClient) writeMinioData(request *pb.MINIORequest) (*pb.Response, error) {
	endpoint := request.GetServer() + ":" + request.GetPort()
	objectName := request.GetRequestId() + "-" + request.GetDataName()
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(request.GetAccessKeyID(), request.GetSecretAccessKey(), ""),
		Secure: request.GetUseSSL(),
	})
	if err != nil {
		log.Fatalln(err)
	}
	// 检查存储桶是否存在，不存在则创建
	exists, errBucketExists := minioClient.BucketExists(context.Background(), request.GetBucketName())
	if errBucketExists != nil {
		log.Fatalln(errBucketExists)
	}
	if !exists {
		err = minioClient.MakeBucket(context.Background(), request.GetBucketName(), minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Successfully created %s\n", request.GetBucketName())
	}

	// 上传文件到 MinIO
	info, err := minioClient.FPutObject(context.Background(), request.GetBucketName(),
		objectName, request.GetFilePath(), minio.PutObjectOptions{ContentType: request.GetContentType()})
	if err != nil {
		return &pb.Response{
			Success: false,
			Message: "Successfully read data",
		}, nil
	}
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("Successfully uploaded %s with a size of %d bytes.", objectName, info.Size),
	}, nil
}

/**
 * @Description 往db里面写数据
 * @Param
 * @return
 **/
func (sdk *DataServiceClient) writeDBData(ctx context.Context, request *pb.WriterDataRequest) (*pb.Response, error) {
	stream, err := sdk.Client.SendArrowData(ctx)
	if err != nil {
		return nil, err
	}

	if err := stream.Send(request); err != nil {

	}

	// 关闭流并接收响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
}

// DownloadFile 从指定的 URL 下载文件并保存到指定的本地路径
func DownloadFile(url, filePath string) error {
	// 创建 HTTP 请求
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 检查请求状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file: status code %d", resp.StatusCode)
	}

	// 创建本地文件
	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	// 将下载的数据写入文件
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
