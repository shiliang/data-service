/*
	@author: shiliang
	@date: 2024/9/6
	@note: 客户端sdk，供外部服务使用

*
*/
package client

import (
	"bytes"
	"context"
	pb "data-service/generated/datasource"
	"fmt"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
)

type DataServiceClient struct {
	client pb.DataSourceServiceClient
	conn   *grpc.ClientConn
	logger *zap.SugaredLogger
}

/***
  * @Description 创建数据服务组件sdk实例
  * @return DataServiceClient 数据服务客户端
  *
***/
func NewDataServiceClient() *DataServiceClient {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	// 创建data service客户端
	return &DataServiceClient{
		logger: sugar,
	}
}

/**
 * @Description 从服务端读取apache arrow的数据
 * @Param
 * @return
 **/
func (sdk *DataServiceClient) ReadBatchData(ctx context.Context, request *pb.BatchReadRequest) (*pb.Response, error) {
	var serverAddress string
	if request.GetServerInfo().GetNamespace() == "" {
		serverAddress = fmt.Sprintf("%s:%s", request.GetServerInfo().GetServiceName(), request.GetServerInfo().GetServicePort())
	} else {
		serverAddress = fmt.Sprintf("%s.%s.svc.cluster.local:%s", request.GetServerInfo().GetServiceName(),
			request.GetServerInfo().GetNamespace(), request.GetServerInfo().GetServicePort())
	}
	// 建立grpc连接
	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		// 连接失败
		return nil, fmt.Errorf("Failed to connect to gRPC server: %v", err)
	}

	sdk.client = pb.NewDataSourceServiceClient(conn)
	sdk.conn = conn
	requestId := uuid.New().String()
	// 调用服务端ReadData方法
	response, err := sdk.client.ReadBatchData(ctx, request)
	if err == nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	err, _ := sdk.readMinioData(request.GetClientId(), requestId, request.GetMinioServer(), request.GetMinioPort(),
		request.GetMinioAK(), request.MinioAK)
	if err != nil {
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("Failed to read data: %v", err),
		}
	}
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("success to read data"),
	}
}

func (sdk *DataServiceClient) ReadStreamingData(ctx context.Context, request *pb.StreamReadRequest) (*pb.Response, error) {
	stream, err := sdk.client.ReadStreamingData(ctx, request)
	if err != nil {
		sdk.logger.Warnw("Failed to read data", "error", err)
		return nil, fmt.Errorf("error calling ReadStreamingData: %w", err)
	}
	var file *os.File
	filePath := request.FilePath
	switch request.FileType {
	case pb.FileType_FILE_TYPE_CSV:
		file, err = os.Create(filePath + ".csv")
	case pb.FileType_FILE_TYPE_JSON:
		file, err = os.Create(filePath + ".json")
	case pb.FileType_FILE_TYPE_PARQUET:
		file, err = os.Create(filePath + ".parquet")
	default:
		return fmt.Errorf("unsupported file type: %v", request.FileType)
	}

	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()
	// 逐批接收 Arrow 数据
	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error receiving stream: %v", err)
		}
		if err == io.EOF {
			sdk.logger.Info("All data received, closing stream.")
			break
		}

		// 读取 Arrow 数据批次
		buf := bytes.NewReader(resp.ArrowBatch)
		reader, err := ipc.NewReader(buf)
		if err != nil {
			log.Fatalf("Error reading Arrow data: %v", err)
		}

		// 处理 Arrow 批次数据
		record := reader.Record()
		sdk.logger.Debug("Received record: %v", record)
		switch request.FileType {
		case pb.FileType_FILE_TYPE_CSV:

		}
	}
}

// close关闭gRPC连接
func (client *DataServiceClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return client.Close()
	}
	return nil
}

// 从minio中读取数据，转化成文件
func (client *DataServiceClient) readMinioData(request *pb.MINIORequest, objectUrl string) (*pb.Response, error) {
	// 创建MinIO客户端
	minioAddress := fmt.Sprintf("%s:%s", minioServer, minioPort)
	minioClient, err := minio.New(minioAddress, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAK, minioSK, ""),
		Secure: false, // MinIO 未启用 TLS 时为 false
	})
	if err != nil {
		log.Fatalln(err)
	}

	// 从 MinIO 流式读取文件
	bucketName := "arrow-data"
	objectName := fmt.Sprintf("%s/%s/data.arrow", clientId, requestId)
	ctx := context.Background()

	// 获取对象
	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	defer object.Close()

	// 创建本地文件以保存数据
	localFile, err := os.Create("downloaded_data.arrow")
	if err != nil {
		log.Fatalln(err)
	}
	defer localFile.Close()

	// 将 MinIO 中的数据流式写入到本地文件
	bytesWritten, err := io.Copy(localFile, object)
	if err != nil {
		log.Fatalln(err)
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("Failed to read data : %v", err),
		}, err
	}

	fmt.Printf("Successfully downloaded %d bytes from MinIO and saved to downloaded_data.arrow\n", bytesWritten)
	return &pb.Response{
		Success: true,
		Message: "Successfully read data",
	}, nil
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
