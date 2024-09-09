/*
*

	@author: shiliang
	@date: 2024/9/6
	@note: 客户端sdk，供外部服务使用

*
*/
package client

import (
	"context"
	pb "data-service/proto/data-service/datasource"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
)

type DataServiceClient struct {
	client pb.DataSourceServiceClient
	conn   *grpc.ClientConn
}

/*
*

  - @Description 创建数据服务组件sdk实例

  - @return DataServiceClient 数据服务客户端
    *
*/
func NewDataServiceClient() *DataServiceClient {
	// 创建data service客户端
	return &DataServiceClient{}
}

// 服务调用 1.建立连接 2.读取数据 3.断开连接

/**
 * @Description 客户端与服务器创建连接，并测试
 * @Param namespace 命名空间
 * @Param serviceName k8s服务名
 * @Param servicePort k8s端口
 * @return 连接信息，连接成功返回nil
 **/
func (sdk *DataServiceClient) Connect(namespace string, serviceName string, servicePort string) (*pb.Response, error) {
	var serverAddress string
	if namespace == "" {
		serverAddress = fmt.Sprintf("%s:%s", serviceName, servicePort)
	} else {
		serverAddress = fmt.Sprintf("%s.%s.svc.cluster.local:%s", serviceName, namespace, servicePort)
	}
	// 建立grpc连接
	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		// 如果连接失败，返回 ConnectionResponse 并携带错误信息
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("Failed to connect to gRPC server: %v", err),
		}, err
	}

	sdk.client = pb.NewDataSourceServiceClient(conn)
	sdk.conn = conn

	// 连接成功，返回 ConnectionResponse
	return &pb.Response{
		Success: true,
		Message: "Successfully connected to gRPC server",
	}, nil
}

/**
 * @Description 从服务端读取apache arrow的数据
 * @Param
 * @return
 **/
func (sdk *DataServiceClient) ReadData(ctx context.Context, request *pb.ReadRequest) *pb.Response {
	if sdk.client == nil {
		return nil, fmt.Errorf("gRpc client not connected")
	}

	// 调用服务端ReadData方法
	err, _ := sdk.client.ReadData(ctx, request)
	if err == nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	requestId := uuid.New().String()
	err := sdk.readMinioData(request.GetClientId(), requestId, request.GetMinioServer(), request.GetMinioPort(),
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

// close关闭gRPC连接
func (client *DataServiceClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return client.Close()
	}
	return nil
}

// 从minio中读取数据，转化成文件
func (client *DataServiceClient) readMinioData(clientId string, requestId string, minioServer string,
	minioPort string, minioAK string, minioSK string) (*pb.Response, error) {
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
