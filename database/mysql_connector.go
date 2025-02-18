/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接mysql

*
*/
package database

import (
	"crypto/tls"
	"crypto/x509"
	"data-service/common"
	ds "data-service/generated/datasource"
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"time"
)

type MySQLStrategy struct {
	info   *ds.ConnectionInfo
	DB     *sql.DB
	logger *zap.SugaredLogger
}

func NewMySQLStrategy(info *ds.ConnectionInfo) *MySQLStrategy {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	return &MySQLStrategy{
		info:   info,
		logger: sugar,
	}
}

// 配置 TLS，直接使用证书字符串
func setupTLSConfig(certContent string) error {
	// 创建一个证书池
	rootCertPool := x509.NewCertPool()

	// 将 PEM 格式的证书字符串添加到证书池中
	if ok := rootCertPool.AppendCertsFromPEM([]byte(certContent)); !ok {
		return fmt.Errorf("failed to append PEM certificate to pool")
	}

	// 注册自定义的 TLS 配置
	err := mysql.RegisterTLSConfig(common.MYSQL_TLS_CONFIG, &tls.Config{
		RootCAs: rootCertPool,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQLStrategy) ConnectToDB() error {
	// 直接使用证书字符串设置 TLS 配置
	err := setupTLSConfig(m.info.TlsCert)
	if err != nil {
		m.logger.Fatalf("Failed to setup TLS: %v", err)
		return fmt.Errorf("failed to setup TLS: %v", err)
	}
	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s?tls=%s", m.info.User, m.info.Host, m.info.Port,
		m.info.DbName, common.MYSQL_TLS_CONFIG)
	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		m.logger.Errorf("Failed to connect to MySQL: %v", err)
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// 检查连接是否成功
	err = db.Ping()
	if err != nil {
		m.logger.Errorf("Failed to ping MySQL: %v", err)
		return fmt.Errorf("failed to ping MySQL: %v", err)
	}
	m.DB = db
	m.logger.Info("Successfully connected to MySQL")
	return nil
}

func (m *MySQLStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	// 构造 MySQL DSN 数据源名称，包含用户名、密码和连接信息
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", info.User, info.Password, info.Host, info.Port, info.DbName)

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		m.logger.Errorf("Failed to connect to MySQL: %v", err)
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// 成功连接后，保存数据库实例
	m.DB = db
	m.logger.Info("Successfully connected to MySQL with username and password")
	return nil
}

func (m *MySQLStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	m.logger.Infof("Executing query: %s with args: %v\n", sqlQuery, args)
	// 执行查询，args 用于绑定 SQL 查询中的占位符
	rows, err := m.DB.Query(sqlQuery, args...)
	if err != nil {
		m.logger.Errorf("Query failed: %v\n", err)
		return nil, err
	}

	return rows, nil
}

func (m *MySQLStrategy) Close() error {
	// 检查数据库连接是否已经初始化
	if m.DB != nil {
		err := m.DB.Close()
		if err != nil {
			m.logger.Errorf("Failed to close MySQL connection: %v", err)
			return err
		}
		m.logger.Info("MySQL connection closed successfully")
		return nil
	}
	// 如果数据库连接未初始化，直接返回 nil
	m.logger.Warn("Attempted to close a non-initialized DB connection")
	return nil
}

func (m *MySQLStrategy) GetJdbcUrl() string {
	// 构建 JDBC URL
	jdbcUrl := fmt.Sprintf(
		"jdbc:mysql://%s:%d/%s?user=%s&password=%s",
		m.info.Host,
		m.info.Port,
		m.info.DbName,
		m.info.User,     // 添加用户名
		m.info.Password, // 添加密码
	)
	return jdbcUrl
}

// 从数据库游标中按行读取数据，并构建当前批次的 Arrow Record
func (m *MySQLStrategy) RowsToArrowBatch(rows *sql.Rows, batchSize int) (arrow.Record, error) {
	if rows == nil {
		return nil, fmt.Errorf("no rows to convert")
	}

	// 创建内存分配器
	pool := memory.NewGoAllocator()

	// 获取列名
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// 获取每列的类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %v", err)
	}

	// 构建 Arrow Schema
	var fields []arrow.Field
	for i, col := range cols {
		arrowType, err := sqlTypeToArrowType(colTypes[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert SQL type to Arrow type: %v", err)
		}
		fields = append(fields, arrow.Field{Name: col, Type: arrowType})
	}
	schema := arrow.NewSchema(fields, nil)

	// 创建 Arrow RecordBuilder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// 准备存储行数据的容器
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}
	rowCount := 0
	// 遍历 rows 并填充 Arrow Builder
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		// 将值添加到 Arrow Builder 中
		for i, val := range values {
			switch b := builder.Field(i).(type) {
			case *array.StringBuilder:
				// 检查是否为 []byte 类型，并转换为字符串
				if val == nil {
					b.AppendNull()
				} else if byteVal, ok := val.([]byte); ok {
					b.Append(string(byteVal)) // 转换为字符串
				} else {
					b.Append(fmt.Sprintf("%v", val)) // 其他类型格式化为字符串
				}
			case *array.Int64Builder:
				if val == nil {
					b.AppendNull()
				} else if intValue, ok := val.(int64); ok {
					b.Append(intValue)
				} else {
					b.AppendNull()
				}
			case *array.Time32Builder:
				// 处理 TIME 类型数据（格式如 16:38:06）
				if val == nil {
					b.AppendNull()
				} else if strVal, ok := val.(string); ok {
					parsedTime, err := time.Parse("15:04:05", strVal)
					if err == nil {
						b.Append(arrow.Time32(parsedTime.Hour()*3600 + parsedTime.Minute()*60 + parsedTime.Second()))
					} else {
						b.AppendNull()
					}
				} else {
					b.AppendNull()
				}
			case *array.TimestampBuilder:
				// 处理 TIME 或 DATETIME 类型的数据 (精度：纳秒)
				if timeVal, ok := val.(time.Time); ok {
					b.Append(arrow.Timestamp(timeVal.UnixNano()))
				} else {
					b.AppendNull()
				}
			default:
				return nil, fmt.Errorf("unsupported builder type: %T", b)
			}
		}
		rowCount++
		if rowCount >= batchSize {
			break // 达到批次大小，结束循环，发送给客户端
		}
	}

	// 创建 Arrow 批次 (Record)
	record := builder.NewRecord()
	return record, nil
}

// 将 SQL 列类型转换为 Arrow 类型
func sqlTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	switch colType.DatabaseTypeName() {
	case "VARCHAR", "TEXT", "CHAR":
		return arrow.BinaryTypes.String, nil
	case "INT", "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT", "DOUBLE", "DECIMAL":
		return arrow.PrimitiveTypes.Float64, nil
	case "TIME":
		return arrow.FixedWidthTypes.Time32s, nil
	case "YEAR":
		// YEAR 类型通常可以用 Int64 来表示
		return arrow.PrimitiveTypes.Int64, nil
	// 处理其他类型
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType.DatabaseTypeName())
	}
}
