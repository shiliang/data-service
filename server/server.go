package main

import (
	"context"
	pb "data-service/generated/datasource"
	"data-service/utils"
	"fmt"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
)

type server struct {
}

/**
 * @Description 批处理的数据，spark读到数据后，以file.arrow格式存储到minio上，再由客户端流式读取
 * @Param
 * @return
 **/
func (s server) ReadBatchData(ctx context.Context, request *pb.BatchReadRequest) (*pb.Response, error) {
	assetName := request.GetAssetName()
	chainId := request.GetRequestId()
	requestId := request.GetRequestId()
	// 用资产名称取数据库连接信息
	product_data_set := utils.GetDatasourceByAssetName(requestId, assetName, chainId)
	// 初始化k8s客户端
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// 创建 Spark Pod
	pod := &corev1.Pod{}
}

func (s server) ReadStreamingData(request *pb.StreamReadRequest, g grpc.ServerStreamingServer[pb.ArrowDataResponse]) error {
	//TODO implement me
	panic("implement me")
}

func (s server) mustEmbedUnimplementedDataSourceServiceServer() {
	//TODO implement me
	panic("implement me")
}

func main() {
	listen, err := net.Listen("tcp", ":8580")
}
