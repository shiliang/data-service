// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v4.25.0--rc2
// source: data_source.proto

package datasource

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	DataSourceService_ReadBatchData_FullMethodName     = "/datasource.DataSourceService/ReadBatchData"
	DataSourceService_ReadStreamingData_FullMethodName = "/datasource.DataSourceService/ReadStreamingData"
	DataSourceService_SendArrowData_FullMethodName     = "/datasource.DataSourceService/SendArrowData"
	DataSourceService_WriteOSSData_FullMethodName      = "/datasource.DataSourceService/WriteOSSData"
)

// DataSourceServiceClient is the client API for DataSourceService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// 数据源服务，提供连接和数据读取功能
type DataSourceServiceClient interface {
	// 批处理读取数据
	ReadBatchData(ctx context.Context, in *WrappedReadRequest, opts ...grpc.CallOption) (*BatchResponse, error)
	// 流式读取数据，使用流式响应来返回数据
	ReadStreamingData(ctx context.Context, in *StreamReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ArrowDataBatch], error)
	// 从客户端发送arrow数据到服务端
	SendArrowData(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[WriterDataRequest, Response], error)
	// 写入OSS数据
	WriteOSSData(ctx context.Context, in *OSSWriteRequest, opts ...grpc.CallOption) (*Response, error)
}

type dataSourceServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDataSourceServiceClient(cc grpc.ClientConnInterface) DataSourceServiceClient {
	return &dataSourceServiceClient{cc}
}

func (c *dataSourceServiceClient) ReadBatchData(ctx context.Context, in *WrappedReadRequest, opts ...grpc.CallOption) (*BatchResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(BatchResponse)
	err := c.cc.Invoke(ctx, DataSourceService_ReadBatchData_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataSourceServiceClient) ReadStreamingData(ctx context.Context, in *StreamReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ArrowDataBatch], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DataSourceService_ServiceDesc.Streams[0], DataSourceService_ReadStreamingData_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[StreamReadRequest, ArrowDataBatch]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataSourceService_ReadStreamingDataClient = grpc.ServerStreamingClient[ArrowDataBatch]

func (c *dataSourceServiceClient) SendArrowData(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[WriterDataRequest, Response], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DataSourceService_ServiceDesc.Streams[1], DataSourceService_SendArrowData_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[WriterDataRequest, Response]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataSourceService_SendArrowDataClient = grpc.ClientStreamingClient[WriterDataRequest, Response]

func (c *dataSourceServiceClient) WriteOSSData(ctx context.Context, in *OSSWriteRequest, opts ...grpc.CallOption) (*Response, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Response)
	err := c.cc.Invoke(ctx, DataSourceService_WriteOSSData_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DataSourceServiceServer is the server API for DataSourceService service.
// All implementations must embed UnimplementedDataSourceServiceServer
// for forward compatibility.
//
// 数据源服务，提供连接和数据读取功能
type DataSourceServiceServer interface {
	// 批处理读取数据
	ReadBatchData(context.Context, *WrappedReadRequest) (*BatchResponse, error)
	// 流式读取数据，使用流式响应来返回数据
	ReadStreamingData(*StreamReadRequest, grpc.ServerStreamingServer[ArrowDataBatch]) error
	// 从客户端发送arrow数据到服务端
	SendArrowData(grpc.ClientStreamingServer[WriterDataRequest, Response]) error
	// 写入OSS数据
	WriteOSSData(context.Context, *OSSWriteRequest) (*Response, error)
	mustEmbedUnimplementedDataSourceServiceServer()
}

// UnimplementedDataSourceServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedDataSourceServiceServer struct{}

func (UnimplementedDataSourceServiceServer) ReadBatchData(context.Context, *WrappedReadRequest) (*BatchResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadBatchData not implemented")
}
func (UnimplementedDataSourceServiceServer) ReadStreamingData(*StreamReadRequest, grpc.ServerStreamingServer[ArrowDataBatch]) error {
	return status.Errorf(codes.Unimplemented, "method ReadStreamingData not implemented")
}
func (UnimplementedDataSourceServiceServer) SendArrowData(grpc.ClientStreamingServer[WriterDataRequest, Response]) error {
	return status.Errorf(codes.Unimplemented, "method SendArrowData not implemented")
}
func (UnimplementedDataSourceServiceServer) WriteOSSData(context.Context, *OSSWriteRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteOSSData not implemented")
}
func (UnimplementedDataSourceServiceServer) mustEmbedUnimplementedDataSourceServiceServer() {}
func (UnimplementedDataSourceServiceServer) testEmbeddedByValue()                           {}

// UnsafeDataSourceServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataSourceServiceServer will
// result in compilation errors.
type UnsafeDataSourceServiceServer interface {
	mustEmbedUnimplementedDataSourceServiceServer()
}

func RegisterDataSourceServiceServer(s grpc.ServiceRegistrar, srv DataSourceServiceServer) {
	// If the following call pancis, it indicates UnimplementedDataSourceServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&DataSourceService_ServiceDesc, srv)
}

func _DataSourceService_ReadBatchData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WrappedReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataSourceServiceServer).ReadBatchData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DataSourceService_ReadBatchData_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataSourceServiceServer).ReadBatchData(ctx, req.(*WrappedReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataSourceService_ReadStreamingData_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamReadRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DataSourceServiceServer).ReadStreamingData(m, &grpc.GenericServerStream[StreamReadRequest, ArrowDataBatch]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataSourceService_ReadStreamingDataServer = grpc.ServerStreamingServer[ArrowDataBatch]

func _DataSourceService_SendArrowData_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DataSourceServiceServer).SendArrowData(&grpc.GenericServerStream[WriterDataRequest, Response]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataSourceService_SendArrowDataServer = grpc.ClientStreamingServer[WriterDataRequest, Response]

func _DataSourceService_WriteOSSData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OSSWriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataSourceServiceServer).WriteOSSData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DataSourceService_WriteOSSData_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataSourceServiceServer).WriteOSSData(ctx, req.(*OSSWriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DataSourceService_ServiceDesc is the grpc.ServiceDesc for DataSourceService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataSourceService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "datasource.DataSourceService",
	HandlerType: (*DataSourceServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReadBatchData",
			Handler:    _DataSourceService_ReadBatchData_Handler,
		},
		{
			MethodName: "WriteOSSData",
			Handler:    _DataSourceService_WriteOSSData_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ReadStreamingData",
			Handler:       _DataSourceService_ReadStreamingData_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SendArrowData",
			Handler:       _DataSourceService_SendArrowData_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "data_source.proto",
}
