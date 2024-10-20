package utils

import (
	pb "data-service/generated/datasource"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"strings"
)

type ConnectionInfo struct {
	Username string
	Password string
	IP       string
	Port     string
}

/**
 * @Description 解析mysql的连接信息，dsn="username:password@tcp(host:port)/dbname?param=value"
 * @Param
 * @return
 **/
func ParseMySQL(dsn string) (*ConnectionInfo, error) {
	// 获取用户名和密码
	// 分割用户名和密码部分
	parts := strings.Split(dsn, "@")
	if len(parts) != 2 {
		return nil, errors.New("invalid DSN format")
	}

	userPass := strings.Split(parts[0], ":")
	if len(userPass) != 2 {
		return nil, errors.New("invalid user:password format")
	}

	username := userPass[0]
	password := userPass[1]

	// 分割 IP 和端口部分
	addrPart := strings.Split(parts[1], "tcp(")
	if len(addrPart) != 2 {
		return nil, errors.New("invalid DSN format for address")
	}

	address := strings.Split(addrPart[1], ")")[0]
	ipPort := strings.Split(address, ":")
	if len(ipPort) != 2 {
		return nil, errors.New("invalid IP:port format")
	}

	ip := ipPort[0]
	port := ipPort[1]

	return &ConnectionInfo{
		Username: username,
		Password: password,
		IP:       ip,
		Port:     port,
	}, nil
}

// columnValue 从 arrow.Array 中获取相应的值
func ColumnValue(column arrow.Array) (interface{}, error) {
	switch column.DataType().ID() {
	case arrow.INT32:
		return column.(*array.Int32).Value(0), nil
	case arrow.STRING:
		return column.(*array.String).Value(0), nil
	default:
		return nil, fmt.Errorf("unsupported column type: %v", column.DataType().ID())
	}
}

func ExtractRowData(record arrow.Record) ([]interface{}, error) {
	argsBatch := []interface{}{}
	numRows := record.NumRows() // 获取行数

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ { // 使用 int64
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			column := record.Column(colIdx)
			var value interface{}

			switch column.DataType().ID() {
			case arrow.INT32:
				int32Array := column.(*array.Int32)
				value = int32Array.Value(int(rowIdx)) // 转换为 int
			case arrow.STRING:
				stringArray := column.(*array.String)
				value = stringArray.Value(int(rowIdx)) // 转换为 int
			// 可以根据需要添加更多类型的支持
			default:
				return nil, fmt.Errorf("unsupported column type: %v", column.DataType().ID())
			}

			argsBatch = append(argsBatch, value)
		}
	}
	return argsBatch, nil
}

// GenerateInsertSQL 生成 SQL 插入语句
func GenerateInsertSQL(tableName string, rowData []interface{}, schema *arrow.Schema, dbType pb.DataSourceType) (string, error) {
	numCols := schema.NumFields()
	if len(rowData)%numCols != 0 {
		return "", fmt.Errorf("row data length (%d) does not match schema length (%d)", len(rowData), numCols)
	}

	// 构建列名
	var columns []string
	for i := 0; i < numCols; i++ {
		columns = append(columns, schema.Field(i).Name)
	}
	columnsStr := strings.Join(columns, ", ")

	// 构建占位符与插入值
	var placeholders []string
	var values []string
	rowCount := len(rowData) / numCols

	if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE {
		// Kingbase 占位符从 $1 开始递增
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			var singleRowPlaceholders []string
			for colIdx := 0; colIdx < numCols; colIdx++ {
				singleRowPlaceholders = append(singleRowPlaceholders, fmt.Sprintf("$%d", rowIdx*numCols+colIdx+1))
				values = append(values, fmt.Sprintf("%v", rowData[rowIdx*numCols+colIdx]))
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(singleRowPlaceholders, ", ")))
		}
	} else if dbType == pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL {
		// MySQL 占位符使用问号
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			var singleRowPlaceholders []string
			for colIdx := 0; colIdx < numCols; colIdx++ {
				singleRowPlaceholders = append(singleRowPlaceholders, "?")
				values = append(values, fmt.Sprintf("%v", rowData[rowIdx*numCols+colIdx]))
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(singleRowPlaceholders, ", ")))
		}
	}

	placeholdersStr := strings.Join(placeholders, ", ")

	// 构建 SQL 语句
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnsStr, placeholdersStr)
	return sql, nil
}
