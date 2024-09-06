package server

import (
	"context"
	pb "data-service/proto/data-service/datasource"
	"log"
	"net"
)

type server struct {
}

func (s server) ConnectToDataSource(ctx context.Context, request *pb.ConnectionRequest) (*pb.ConnectionResponse, error) {
	log.Printf("Connecting to data source: %s", request.GetDataSourceType())
}

func (s server) ReadData(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) DisconnectFromDataSource(ctx context.Context, request *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
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
