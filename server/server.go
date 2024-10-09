package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/shiliang/data-service/common"
	"github.com/shiliang/data-service/config"
	"github.com/shiliang/data-service/database"
	pb "github.com/shiliang/data-service/generated/datasource"
	log2 "github.com/shiliang/data-service/log"
	"github.com/shiliang/data-service/server/routes"
	"github.com/shiliang/data-service/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
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

func (s Server) WriteInternalData(g grpc.ClientStreamingServer[pb.WriterInternalDataRequest, pb.Response]) error {
	conf := config.GetConfigMap()
	dbType := utils.ConvertDBType(conf.Dbms.Type)

	for {
		request, err := g.Recv()
		if err != nil {
			return err
		}
		connInfo := &pb.ConnectionInfo{Host: conf.Dbms.Host,
			Port:     conf.Dbms.Port,
			User:     conf.Dbms.User,
			DbName:   request.DbName,
			Password: conf.Dbms.Password,
		}
		// 处理请求，拿取数据
		reader := bytes.NewReader(request.ArrowBatch)
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
		dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
		if err := dbStrategy.ConnectToDBWithPass(connInfo); err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}
		db := database.GetDB(dbStrategy)
		if err := insertArrowDataInBatches(db, request.GetTableName(), schema, ipcReader, dbType); err != nil {
			return fmt.Errorf("failed to insert Arrow data: %v", err)
		}
	}
}

func (s Server) ReadStreamingData(request *pb.StreamReadRequest, g grpc.ServerStreamingServer[pb.ArrowResponse]) error {
	// 解析客户端请求参数
	// 获取要连接的数据库信息
	connInfo, err := utils.GetDatasourceByAssetName(request.GetRequestId(), request.AssetName,
		request.ChainInfoId, request.PlatformId)

	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	// 从数据源中读取arrow数据流
	dbStrategy, _ := database.DatabaseFactory(dbType, connInfo)
	if err := dbStrategy.ConnectToDB(); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	// 拼接sql，执行数据库查询
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(request.DbFields, ","), connInfo.TableName)
	rows, err := dbStrategy.Query(query)
	if err != nil {
		return fmt.Errorf("error executing query: %v", err)
	}
	// 返回arrow流
	for {
		// 读取 Arrow 批次
		record, err := dbStrategy.RowsToArrowBatch(rows)
		if err == io.EOF {
			break // 没有更多数据
		}
		if err != nil {
			return fmt.Errorf("error reading Arrow batch: %v", err)
		}

		// 使用 IPC Writer 将 Arrow 批次序列化为字节流
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %v", err)
		}

		// 发送 ArrowResponse 给客户端
		response := &pb.ArrowResponse{
			ArrowBatch: buf.Bytes(),
		}
		if err := g.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}
	}
	return nil
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
		connInfo, err := utils.GetDatasourceByAssetName(request.GetRequestId(), request.GetRequest().GetAssetName(),
			request.GetRequest().GetChainInfoId(), request.GetRequest().PlatformId)
		if err != nil {
			return fmt.Errorf("failed to get product data set: %v", err)
		}
		dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
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
		table := connInfo.TableName
		dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
		if err := dbStrategy.ConnectToDB(); err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}
		db := database.GetDB(dbStrategy)
		if err := insertArrowDataInBatches(db, table, schema, ipcReader, dbType); err != nil {
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
	platformId := request.GetRequest().GetPlatformId()
	// 用资产名称取数据库连接信息
	connInfo, err := utils.GetDatasourceByAssetName(requestId, assetName, chainId, platformId)
	if err != nil {
		return nil, err
	}
	dbType := utils.ConvertDataSourceType(connInfo.Dbtype)
	dbStrategy, err := database.DatabaseFactory(dbType, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to get database strategy: %v", err)
	}
	jdbcUrl := dbStrategy.GetJdbcUrl()
	// 初始化k8s客户端
	k8s_config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(k8s_config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// 创建 Spark Pod
	podName := "spark-job-" + requestId
	_, err = utils.CreateSparkPod(clientset, "spark", podName, jdbcUrl)
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
	if conf.OSSConfig.Type == "minio" {
		// 创建 MinIO 客户端
		endpoint := fmt.Sprintf("%s:%d", conf.OSSConfig.Host, conf.OSSConfig.Port)
		client, err = minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(conf.OSSConfig.AccessKey, conf.OSSConfig.SecretKey, ""),
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
func insertArrowDataInBatches(db *sql.DB, tableName string, schema *arrow.Schema, ipcReader *ipc.Reader, dbType pb.DataSourceType) error {

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// 3. 批量插入数据
	argsBatch := []interface{}{}
	rowCount := int64(0)
	for ipcReader.Next() {
		record := ipcReader.Record()
		if record == nil || record.NumRows() == 0 {
			continue // 跳过空记录
		}

		args, err := utils.ExtractRowData(record)
		if err != nil {
			tx.Rollback()
			return err
		}
		argsBatch = append(argsBatch, args...)
		rowCount += record.NumRows() // 更新行计数

		// 当达到 batchSize 时，执行批量插入
		if rowCount >= common.BATCH_DATA_SIZE {
			insertSQL, err := utils.GenerateInsertSQL(tableName, argsBatch, schema, dbType)
			_, err = tx.Exec(insertSQL, argsBatch...)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to execute batch insert: %v", err)
			}
			argsBatch = []interface{}{} // 清空批次数据
			rowCount = 0
		}
	}

	// 如果最后一批数据未达到 batchSize，需要手动插入
	if rowCount > 0 && len(argsBatch) > 0 {
		insertSQL, err := utils.GenerateInsertSQL(tableName, argsBatch, schema, dbType)
		_, err = tx.Exec(insertSQL, argsBatch...)
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
	log2.InitLogger()
	// 创建spark用户权限
	// utils.SetupKubernetesClientAndResources()
	listen, err := net.Listen("tcp", ":8580")
	if err != nil {
		log2.Logger.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	dataService := &Server{logger: log2.Logger}
	// 注册服务
	pb.RegisterDataSourceServiceServer(grpcServer, dataService)
	log2.Logger.Infof("gRPC server running at %v", listen.Addr())
	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log2.Logger.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// 注册路由
	routes.RegisterRoutes()
	// 启动HTTP服务器
	conf := config.GetConfigMap()
	log2.Logger.Infof("HTTP server running at %s", conf.HttpServiceConfig.Port)
	if err = http.ListenAndServe(":"+fmt.Sprintf("%d", conf.HttpServiceConfig.Port), nil); err != nil {
		log2.Logger.Fatalf("failed to serve: %v", err)
	}
}
