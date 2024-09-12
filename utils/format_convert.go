/*
*

	@author: shiliang
	@date: 2024/9/9
	@note:

*
*/
package utils

import (
	"bytes"
	pb "data-service/generated/datasource"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/ipc"
	"io"
)

// 转换函数，支持不同格式
func ConvertArrow(data *array.Record, format string) ([]byte, error) {
	// 如果 format 为空，返回原始数据
	if format == "" {
		return nil, nil
	}

	var output []byte
	switch format {
	case "csv":
		output = convertToCSV(data)
	case "json":
		output = convertToJSON(data)
	default:
		return nil, fmt.Errorf("不支持的格式: %s", format)
	}

	return output, nil
}

// 假设的 CSV 转换函数
func convertToCSV(stream pb.DataSourceService_ReadStreamingDataClient, writer *csv.Writer) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			sdk.logger.Info("All data received, closing stream.")
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving stream: %w", err)
		}

		// 读取 Arrow 数据批次
		buf := bytes.NewReader(resp.ArrowBatch)
		reader, err := ipc.NewReader(buf)
		if err != nil {
			return fmt.Errorf("error reading Arrow data: %w", err)
		}

		// 逐行写入 CSV 文件
		for i := 0; i < int(reader.Record().NumRows()); i++ {
			var row []string
			for j := 0; j < int(reader.Record().NumCols()); j++ {
				value := fmt.Sprintf("%v", reader.Record().Column(j))
				row = append(row, value)
			}
			writer.Write(row)
		}
		writer.Flush() // 确保批次数据被写入文件
	}
}

// 假设的 JSON 转换函数
func convertToJSON(data *array.Record) []byte {
	// 在这里实现将 Arrow 转换为 JSON 的逻辑
	// 这里只是简单示例，实际需要根据 Arrow 表结构实现
	return []byte("{\"column1\": \"value1\", \"column2\": \"value2\"}\n")
}
