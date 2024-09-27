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
	"fmt"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/ipc"
	pb "github.com/shiliang/data-service/generated/datasource"
	"go.uber.org/zap"
	"io"
	"os"
)

// 转换函数，支持不同格式
func ConvertDataToFile(stream pb.DataSourceService_ReadStreamingDataClient, filePath string,
	logger *zap.SugaredLogger, format pb.FileType) error {
	switch format {
	case pb.FileType_FILE_TYPE_CSV:
		if err := convertToCSV(stream, filePath, logger); err != nil {
			return fmt.Errorf("convertToCSV: %w", err)
		}
	case pb.FileType_FILE_TYPE_ARROW:
		if err := convertToArrowFile(stream, filePath, logger); err != nil {
			return fmt.Errorf("convertToCSV: %w", err)
		}
	default:
		return fmt.Errorf("不支持的格式: %s", format)
	}

	return nil
}

// 把arrow数据流转化为csv格式
func convertToCSV(stream pb.DataSourceService_ReadStreamingDataClient, filePath string, logger *zap.SugaredLogger) error {
	file, err := os.Create(filePath + ".csv")
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Errorf("error closing file: %v", err)
		}
	}()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			logger.Info("All data received, closing stream.")
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

		writer := csv.NewWriter(file, reader.Schema())
		if err := writer.Write(reader.Record()); err != nil {
			logger.Errorf("error writing record to file: %v", err)
			return fmt.Errorf("error writing record to file: %w", err)
		}

		err = writer.Flush()
		if err != nil {
			logger.Errorf("error flushing record to file: %v", err)
			return fmt.Errorf("error flushing record to file: %w", err)
		} // 确保批次数据被写入文件
		reader.Release() // 确保资源释放
	}
	return nil
}

// convertToArrowFile 将流式接收到的 Arrow 数据保存为 .arrow 文件
func convertToArrowFile(stream pb.DataSourceService_ReadStreamingDataClient, filePath string, logger *zap.SugaredLogger) error {
	// 创建输出的 Arrow 文件
	file, err := os.Create(filePath + ".arrow")
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Errorf("error closing file: %v", err)
		}
	}()

	// 使用 *ipc.FileWriter 代替 *ipc.Writer
	var writer *ipc.FileWriter

	for {
		// 接收流中的 Arrow 数据批次
		resp, err := stream.Recv()
		if err == io.EOF {
			logger.Info("All data received, closing stream.")
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

		// 第一次处理时，创建 Arrow 文件写入器，基于读取到的 schema
		if writer == nil {
			writer, err = ipc.NewFileWriter(file, ipc.WithSchema(reader.Schema()))
			if err != nil {
				return fmt.Errorf("failed to create Arrow file writer: %w", err)
			}
			defer writer.Close() // 确保资源被释放
		}

		// 将每个批次数据写入 Arrow 文件
		for reader.Next() {
			record := reader.Record()
			if err := writer.Write(record); err != nil {
				logger.Errorf("error writing record to file: %v", err)
				return fmt.Errorf("error writing record to Arrow file: %w", err)
			}
		}

		// 确保批次数据资源释放
		reader.Release()
	}

	return nil
}
