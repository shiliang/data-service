package client_test

import (
	"context"
	"data-service/client"
	pb "data-service/generated/datasource"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadStreamingData(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDataServiceClient := client.NewMockDataSourceServiceClient(mockCtrl)

	sdk := &client.DataServiceClient{}
	sdk.SetClient(mockDataServiceClient)

	ctx := context.Background()
	filePath := "test_path"
	fileType := pb.FileType_FILE_TYPE_CSV
	request := &pb.StreamReadRequest{
		FilePath: filePath,
		FileType: fileType,
	}

	expectedResponse := &pb.Response{Success: true}

	mockDataServiceClient.EXPECT().
		ReadStreamingData(ctx, request).
		Return(nil, nil).
		Times(1)

	// Here we assume that the ConvertDataToFile function works correctly.
	// We do not mock it because it is not the focus of our test.
	// If ConvertDataToFile has side effects that need to be observed or controlled,
	// we might need to refactor it or mock it as well.

	response, err := sdk.ReadStreamingData(ctx, request)

	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, response)
}
