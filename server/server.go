package main

import (
	"context"
	pb "data-service/generated/datasource"
	"data-service/utils"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
)

type Server struct {
	logger *zap.SugaredLogger
	pb.UnimplementedDataSourceServiceServer
}

func (s Server) ReadBatchData(ctx context.Context, request *pb.WrappedReadRequest) (*pb.BatchResponse, error) {
	assetName := request.GetRequest().GetAssetName()
	chainId := request.GetRequest().GetChainInfoId()
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

	// 创建 Spark Pod，并将数据源信息作为环境变量传递
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spark-container",
					Image: "spark:latest", // 替换为实际的 Spark 镜像
					Args: []string{
						"/opt/spark/bin/spark-submit",
						"--class", "org.apache.spark.examples.SparkPi",
						"--master", "k8s://https://kubernetes.default.svc",
						"--deploy-mode", "cluster",
						"--conf", fmt.Sprintf("spark.executor.instances=2"),
						"--conf", fmt.Sprintf("spark.kubernetes.container.image=spark:latest"),
						"--conf", fmt.Sprintf("spark.datasource.jdbc.url=%s", jdbcUrl),
						"--conf", fmt.Sprintf("spark.datasource.jdbc.username=%s", username),
						"--conf", fmt.Sprintf("spark.datasource.jdbc.password=%s", password),
						"local:///opt/spark/examples/jars/spark-examples_2.12-3.0.1.jar",
					},
					Env: []corev1.EnvVar{
						{
							Name:  "JDBC_URL",
							Value: jdbcUrl,
						},
						{
							Name:  "DB_USERNAME",
							Value: username,
						},
						{
							Name:  "DB_PASSWORD",
							Value: password,
						},
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "spark-port",
							ContainerPort: 7077,
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	// 创建 Pod
	pod, err = clientset.CoreV1().Pods("default").Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		s.logger.Fatalf("Failed to create Pod: %v", err)
	}
}

func (s Server) ReadStreamingData(request *pb.StreamReadRequest, g grpc.ServerStreamingServer[pb.ArrowDataBatch]) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) SendArrowData(g grpc.ClientStreamingServer[pb.WriterDataRequest, pb.Response]) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) mustEmbedUnimplementedDataSourceServiceServer() {
	//TODO implement me
	panic("implement me")
}

func main() {
	// 初始化 logger
	logger, _ := zap.NewProduction() // 使用生产环境配置，或者你可以使用 zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer logger.Sync()

	listen, err := net.Listen("tcp", ":8580")
	if err != nil {
		sugaredLogger.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	dataService := &Server{logger: sugaredLogger}
	// 注册服务
	pb.RegisterDataSourceServiceServer(grpcServer, dataService)
	sugaredLogger.Infof("gRPC server running at %v", listen.Addr())
	if err := grpcServer.Serve(listen); err != nil {
		sugaredLogger.Fatalf("failed to serve: %v", err)
	}
}
