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
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
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
func NewDataServiceClient(ctx context.Context, serverInfo *pb.ServerInfo) (*DataServiceClient, error) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
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
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	client := pb.NewDataSourceServiceClient(conn)

	// 创建data service客户端
	return &DataServiceClient{
		logger: sugar,
		ctx:    ctx,
		client: client,
	}, nil
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
	wrappedRequest := &pb.WrappedReadRequest{
		Request:   request,
		RequestId: uuid.New().String(),
	}
	// 调用服务端ReadData方法
	response, err := sdk.client.ReadBatchData(ctx, wrappedRequest)
	if err == nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
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
func (sdk *DataServiceClient) ReadStreamingData(ctx context.Context, request *pb.StreamReadRequest) *pb.Response {
	stream, err := sdk.client.ReadStreamingData(ctx, request)
	if err != nil {
		sdk.logger.Warnw("Failed to read data", "error", err)
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("Error calling ReadStreamingData: %v", err),
		}
	}

	switch request.FileType {
	case pb.FileType_FILE_TYPE_CSV:
		filePath := request.FilePath
		err := utils.ConvertDataToFile(stream, filePath, sdk.logger, pb.FileType_FILE_TYPE_CSV)
		if err != nil {
			return &pb.Response{
				Success: false,
				Message: fmt.Sprintf("Error converting data to CSV: %v", err),
			}
		}
	case pb.FileType_FILE_TYPE_ARROW:
		filePath := request.FilePath
		err := utils.ConvertDataToFile(stream, filePath, sdk.logger, pb.FileType_FILE_TYPE_ARROW)
		if err != nil {
			return &pb.Response{
				Success: false,
				Message: fmt.Sprintf("Error converting data to arrow: %v", err),
			}
		}
	default:
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("unsupported file type: %v", request.FileType),
		}
	}
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("unsupported file type: %v", request.FileType),
	}
}

// ReadStream 用于从数据源读取流式数据。调用完此方法后，需调用Close方法关闭客户端
// 该方法首先尝试连接数据服务，连接成功后，使用提供的请求调用 ReadStreamingData 方法。
// 如果在连接或读取过程中发生错误，该方法将返回错误。
// 参数:
//   - ctx: 上下文，用于传递请求范围的配置和值。
//   - request: 包含读取流数据所需信息的请求对象。
//
// 返回值:
//   - pb.DataSourceService_ReadStreamingDataClient: 一个客户端流，用于处理返回的流式数据。
//   - error: 如果连接或读取过程中发生错误，则返回错误。
func (sdk *DataServiceClient) ReadStream(ctx context.Context, request *pb.StreamReadRequest) (pb.DataSourceService_ReadStreamingDataClient, error) {
	// 使用提供的请求调用 ReadStreamingData 方法。
	stream, err := sdk.client.ReadStreamingData(ctx, request)
	if err != nil {
		// 如果读取数据失败，记录日志并返回错误。
		sdk.logger.Warnw("Failed to read data", "error", err)
		return nil, fmt.Errorf("error calling ReadStreamingData: %w", err)
	}
	// 返回成功的数据流。
	return stream, nil
}

// 关闭gRPC连接
func (client *DataServiceClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("failed to disconnect to gRPC server: %v", err)
	}
	return nil
}

// 写入数据文件到OSS，文件名为requestId + taskId
func (sdk *DataServiceClient) WriteOSSData(ctx context.Context, request *pb.OSSWriteRequest) (*pb.Response, error) {
	response, err := sdk.client.WriteOSSData(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

/**
 * @Description 往外部数据源写数据
 * @Param
 * @return
 **/
func (sdk *DataServiceClient) writeDBData(ctx context.Context, request *pb.WriterDataRequest) *pb.Response {
	stream, err := sdk.client.SendArrowData(ctx)
	// 生成requestId
	requestId := uuid.New().String()
	wrappedRequest := &pb.WrappedWriterDataRequest{
		Request:   request,
		RequestId: requestId,
	}
	if err != nil {
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("failed to create stream: %v", err),
		}
	}
	if err := stream.Send(wrappedRequest); err != nil {
		return &pb.Response{
			Success: false,
			Message: fmt.Sprintf("failed to send stream: %v", err),
		}
	}
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("success to write data: %v", err),
	}
}

// 往内置数据库写数据
/*func (sdk *DataServiceClient) writeInternalDBData(ctx context.Context, request *pb.WriterInternalDataRequest) *pb.Response {

}*/

// 读内置数据库数据
/*func (sdk *DataServiceClient) readInternalDBData(ctx context.Context, request *pb.ReadInternalDataRequest) *pb.Response {

}*/

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
