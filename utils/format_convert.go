/*
*

	@author: shiliang
	@date: 2024/9/9
	@note:

*
*/
package utils

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
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
func convertToCSV(data *array.Record) []byte {
	// 在这里实现将 Arrow 转换为 CSV 的逻辑
	// 这里只是简单示例，实际需要根据 Arrow 表结构实现
	return []byte("column1,column2\nvalue1,value2\n")
}

// 假设的 JSON 转换函数
func convertToJSON(data *array.Record) []byte {
	// 在这里实现将 Arrow 转换为 JSON 的逻辑
	// 这里只是简单示例，实际需要根据 Arrow 表结构实现
	return []byte("{\"column1\": \"value1\", \"column2\": \"value2\"}\n")
}
