package main

import (
	pb "data-service/proto/data-service/datasource"
	"data-service/server"
	"google.golang.org/grpc"
	"log"
	"net"
)

func init() {

}

func main() {
	listen, err := net.Listen("tcp", ":8580")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	// 注册服务
	pb.RegisterDataSourceServiceServer(grpcServer, &server.server{})

	log.Println("Server is running on port 8580...")
	if err := grpcServer.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
