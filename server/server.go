package main

import (
	"bytes"
	"context"
	"data-service/config"
	"data-service/database"
	pb "data-service/generated/datasource"
	"data-service/utils"
	"fmt"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"net"
	"os"
)

type Server struct {
	logger *zap.SugaredLogger
}

func (s Server) ReadStreamingData(request *pb.StreamReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) SendArrowData(g grpc.ClientStreamingServer[pb.WrappedWriterDataRequest, pb.Response]) error {
	// 接收arrow数据流发过来的数据
	for {
		request, err := g.Recv()
		if err != nil {
			return err
		}
		// 处理请求，拿取数据
		reader := bytes.NewReader(request.GetRequest().GetArrowBatch())
		// 获取要连接的数据库信息
		product_data_set := utils.GetDatasourceByAssetName(request.GetRequestId(), request.GetRequest().GetAssetName(),
			request.GetRequest().GetChainInfoId())
		dbType := utils.ConvertDataSourceType(product_data_set.GetDbConnInfo().GetType())
		// 使用 Arrow 的内存分配器
		pool := memory.NewGoAllocator()

		// 使用 IPC 文件读取器解析数据
		ipcReader, err := ipc.NewFileReader(reader, ipc.WithAllocator(pool))
		if err != nil {
			log.Fatalf("Failed to create Arrow IPC reader: %v", err)
		}
		defer ipcReader.Close()

		// 获取表结构信息
		schema := ipcReader.Schema()
		database.DatabaseFactory(dbType, product_data_set.DbConnInfo)
	}
}

func (s Server) mustEmbedUnimplementedDataSourceServiceServer() {
	//TODO implement me
	panic("implement me")
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
	// 监控spark作业执行状态，通过回调函数，获取执行结果

}

func (s Server) WriteOSSData(ctx context.Context, request *pb.OSSWriteRequest) (*pb.Response, error) {
	conf := config.GetConfigMap()
	var client, err interface{}
	if conf.OSSType == "minio" {
		// 创建 MinIO 客户端
		client, err = minio.New(conf.OSSEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(conf.AccessKeyID, conf.SecretAccessKey, ""),
			Secure: false,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create MinIO client: %v", err)
		}
	}
	// 生成上传文件名
	fileName := request.GetRequestId() + "_" + request.GetTaskId()
	// 将字节内容写入本地临时文件
	tempFilePath := "/tmp/" + fileName
	if err := os.WriteFile(tempFilePath, request.GetFileContent(), 0644); err != nil {
		return nil, fmt.Errorf("failed to write file: %v", err)
	}
	// 打开临时文件
	file, err := os.Open(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	// 上传文件到 OSS
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}
	client
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
