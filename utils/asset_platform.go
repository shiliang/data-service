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
	pb "data-service/generated/ida"
	"google.golang.org/grpc"
	"log"
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
