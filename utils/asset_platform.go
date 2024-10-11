/*
*

	@author: shiliang
	@date: 2024/9/11
	@note: 获取数据资产相关信息

*
*/
package utils

import (
	"context"
	"fmt"
	pb2 "github.com/shiliang/data-service/generated/datasource"
	pb "github.com/shiliang/data-service/generated/ida"
	"google.golang.org/grpc"
	"log"
	"os"
	"strings"
)

func GetDatasourceByAssetName(requestId string, assetName string, chainId int32, platformId int32) (*pb2.ConnectionInfo, error) {
	IDAServerAddress := os.Getenv("IDA_MANAGE_HOST")
	IDAServerPort := os.Getenv("IDA_MANAGE_PORT")
	idaAddress := IDAServerAddress + ":" + IDAServerPort
	conn, err := grpc.Dial(idaAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	stub := pb.NewMiraIdaAccessClient(conn)
	request := &pb.GetPrivateAssetInfoByEnNameReq{RequestId: requestId, AssetEnName: assetName, ChainInfoId: chainId,
		PlatformId: platformId}
	response, err := stub.GetPrivateAssetInfoByEnName(context.Background(), request)
	if err != nil {
		return nil, err
	}

	if response == nil || response.GetData() == nil {
		return nil, fmt.Errorf("response data is nil")
	}
	productDataSet := response.GetData().ProductInfo.ProductDataSet
	if productDataSet == nil || productDataSet.DbConnInfo == nil {
		return nil, fmt.Errorf("productDataSet or DbConnInfo is nil")
	}
	// 通过connId取具体信息
	connRequest := &pb.GetPrivateDBConnInfoReq{RequestId: requestId, DbConnId: productDataSet.DbConnInfo.DbConnId}
	connResponse, err := stub.GetPrivateDBConnInfo(context.Background(), connRequest)
	if err != nil {
		return nil, err
	}

	if connResponse == nil || connResponse.GetData() == nil {
		return nil, fmt.Errorf("connection response data is nil")
	}
	connInfo := &pb2.ConnectionInfo{
		Host:     connResponse.GetData().Host,
		Port:     connResponse.GetData().Port,
		User:     connResponse.GetData().Username,
		DbName:   connResponse.GetData().DbName,
		Password: connResponse.GetData().Password,
		Dbtype:   connResponse.GetData().Type,
	}
	return connInfo, nil
}

// 转换数据源类型
func ConvertDataSourceType(dbType int32) pb2.DataSourceType {
	switch dbType {
	case 1:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL
	case 2:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_HIVE
	default:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN
	}
}

func ConvertDBType(dbType string) pb2.DataSourceType {
	switch dbType {
	case "mysql":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_MYSQL
	case "kingbase":
		return pb2.DataSourceType_DATA_SOURCE_TYPE_KINGBASE
	default:
		return pb2.DataSourceType_DATA_SOURCE_TYPE_UNKNOWN
	}
}

func GetDbTypeName(dbType int32) string {
	switch dbType {
	case 1:
		return "mysql"
	case 2:
		return "kingbase"
	default:
		return "unknown"
	}
}

// GenerateTLSFile 生成本地的 TLS 证书文件
func GenerateTLSFile(connInfo *pb.DataSetInfo) (string, error) {
	// 组合文件名和路径
	filePath := fmt.Sprintf("./%s.%s", connInfo.DbConnInfo.TlsCertName, connInfo.DbConnInfo.TlsCertExt)

	// 创建文件并写入证书内容
	err := os.WriteFile(filePath, []byte(connInfo.DbConnInfo.TlsCert), 0644)
	if err != nil {
		return "", fmt.Errorf("failed to write TLS certificate file: %v", err)
	}

	return filePath, nil
}

func BuildSelectQuery(request *pb2.BatchReadRequest, tableName string) string {
	// 如果 dbFields 为空，则默认选择所有字段 "*"
	var fields string
	if len(request.DbFields) > 0 {
		fields = strings.Join(request.DbFields, ", ")
	} else {
		fields = "*"
	}

	// 构建 SELECT 语句
	query := fmt.Sprintf("SELECT %s FROM %s;", fields, tableName)
	return query
}
