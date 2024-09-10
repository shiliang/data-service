package main

import (
	"context"
	pb "data-service/proto/data-service/datasource"
	"fmt"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
)

type server struct {
}

func (s server) ReadBatchData(ctx context.Context, request *pb.BatchReadRequest) (*pb.Response, error) {
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
