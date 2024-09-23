package main

import (
	"bytes"
	"context"
	"data-service/common"
	"data-service/config"
	"data-service/database"
	pb "data-service/generated/datasource"
	"data-service/server/routes"
	"data-service/utils"
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

type Server struct {
	logger *zap.SugaredLogger
	pb.UnimplementedDataSourceServiceServer
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
		ipcReader, err := ipc.NewReader(reader, ipc.WithAllocator(pool))
		if err != nil {
			log.Fatalf("Failed to create Arrow IPC reader: %v", err)
		}
		defer ipcReader.Release()

		// 获取表结构信息
		schema := ipcReader.Schema()
		table := product_data_set.TableName
		dbStrategy, err := database.DatabaseFactory(dbType, product_data_set.DbConnInfo)
		if err := dbStrategy.ConnectToDB(); err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}
		db := database.GetDB(dbStrategy)
		if err := insertArrowDataInBatches(db, table, schema, ipcReader); err != nil {
			return fmt.Errorf("failed to insert Arrow data: %v", err)
		}
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
	conf := config.GetConfigMap()
	// 用资产名称取数据库连接信息
	product_data_set := utils.GetDatasourceByAssetName(requestId, assetName, chainId)
	dbType := utils.ConvertDataSourceType(product_data_set.GetDbConnInfo().GetType())
	dbStrategy, err := database.DatabaseFactory(dbType, product_data_set.DbConnInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to get database strategy: %v", err)
	}
	jdbcUrl, err := dbStrategy.GetJdbcUrl()
	// 初始化k8s客户端
	k8s_config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(k8s_config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// 创建 Spark Pod，并将数据源信息作为环境变量传递
	podName := "spark-job-" + requestId
	// 创建 Pod
	_, err = utils.CreateSparkPod(clientset, conf.SparkNamespace, podName, jdbcUrl)
	if err != nil {
		s.logger.Fatalf("Failed to create Pod: %v", err)
	}
	// 监控spark作业执行状态，通过回调函数，获取执行结果
	url := <-routes.MinioUrlChan
	return &pb.BatchResponse{ObjectUrl: url}, nil
}

func (s Server) WriteOSSData(ctx context.Context, request *pb.OSSWriteRequest) (*pb.Response, error) {
	conf := config.GetConfigMap()
	var err interface{}
	var client *minio.Client
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
	// 上传文件到 MinIO
	uploadInfo, err := client.PutObject(ctx, common.BATCH_DATA_BUCKET_NAME, fileName, file, fileInfo.Size(), minio.PutObjectOptions{
		ContentType: "application/octet-stream", // 设置内容类型，可以根据文件类型动态调整
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload file to MinIO: %v", err)
	}

	// 成功上传后，返回 MinIO 上的文件信息
	return &pb.Response{
		Success: true,
		Message: fmt.Sprintf("File uploaded to MinIO successfully: %s (size: %d bytes)", uploadInfo.Key, uploadInfo.Size),
	}, nil

}

// 使用事务批量插入Arrow数据到数据库
func insertArrowDataInBatches(db *sql.DB, tableName string, schema *arrow.Schema, ipcReader *ipc.Reader) error {
	// 1. 拼接 INSERT INTO 的前半部分
	columns := []string{}
	placeholders := []string{}
	for _, field := range schema.Fields() {
		columns = append(columns, field.Name)
		placeholders = append(placeholders, "?")
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// 2. 开始事务
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// 3. 批量插入数据
	argsBatch := []interface{}{}
	rowCount := 0

	for ipcReader.Next() {
		record := ipcReader.Record()

		// 遍历每一列，提取行数据
		for rowIdx := 0; rowIdx < int(record.NumRows()); rowIdx++ {
			for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
				column := record.Column(colIdx)
				value := column.Data()
				argsBatch = append(argsBatch, value)
			}
			rowCount++

			// 当达到 batchSize 时，执行批量插入
			if rowCount >= common.BATCH_DATA_SIZE {
				_, err := tx.Exec(insertSQL, argsBatch...)
				if err != nil {
					tx.Rollback()
					return fmt.Errorf("failed to execute batch insert: %v", err)
				}
				// 清空批次数据
				argsBatch = []interface{}{}
				rowCount = 0
			}
		}
	}

	// 如果最后一批数据未达到 batchSize，需要手动插入
	if rowCount > 0 {
		_, err := tx.Exec(insertSQL, argsBatch...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute final batch insert: %v", err)
		}
	}

	// 4. 提交事务
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Println("Data inserted successfully into", tableName)
	return nil
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

	// 注册路由
	routes.RegisterRoutes()
	// 启动HTTP服务器
	conf := config.GetConfigMap()
	sugaredLogger.Infof("HTTP server running at %s", conf.HttpPort)
	if err = http.ListenAndServe(":"+conf.HttpPort, nil); err != nil {
		sugaredLogger.Fatalf("failed to serve: %v", err)
	}
}
