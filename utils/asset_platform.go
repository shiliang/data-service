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
	config2 "data-service/config"
	pb2 "data-service/generated/datasource"
	pb "data-service/generated/ida"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"os"
)

func GetDatasourceByAssetName(requestId string, assetName string, chainId int32) *pb.DataSetInfo {
	config := config2.GetConfigMap()
	idaAddress := config.IDAServerAddress + ":" + config.IDAServerPort
	conn, err := grpc.Dial(idaAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	stub := pb.NewMiraIdaAccessClient(conn)
	request := &pb.GetPrivateAssetInfoByEnNameReq{RequestId: requestId, AssetEnName: assetName, ChainInfoId: chainId}
	response, err := stub.GetPrivateAssetInfoByEnName(context.Background(), request)
	product_data_set := response.GetData().ProductInfo.ProductDataSet
	return product_data_set
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
