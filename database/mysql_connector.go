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
	"data-service/generated/datasource"
	pb "data-service/generated/ida"
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type MySQLStrategy struct {
	info   *pb.DBConnInfo
	DB     *sql.DB
	logger *zap.SugaredLogger
}

func NewMySQLStrategy(info *pb.DBConnInfo) *MySQLStrategy {
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
	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s?tls=%s", m.info.Username, m.info.Host, m.info.Port,
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

func (m *MySQLStrategy) ConnectToDBWithPass(info *datasource.ConnectionInfo) error {
	// 构造 MySQL DSN 数据源名称，包含用户名、密码和连接信息
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", info.User, info.Password, info.Host, info.Port, info.DbName)

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

func (m *MySQLStrategy) GetJdbcUrl() (string, error) {
	// 设置 TLS 配置，使用从 tls_cert 中提取的内容
	err := setupTLSConfig(m.info.TlsCert)
	if err != nil {
		return "", fmt.Errorf("failed to setup TLS config: %v", err)
	}

	// 构建 JDBC URL
	jdbcUrl := fmt.Sprintf(
		"jdbc:mysql://%s:%d/%s?useSSL=true&requireSSL=true&verifyServerCertificate=true&tls=%s",
		m.info.Host,
		m.info.Port,
		m.info.DbName,
		common.MYSQL_TLS_CONFIG,
	)
	return jdbcUrl, nil
}

func (m *MySQLStrategy) RowsToArrowBatch(rows *sql.Rows) (array.Record, error) {
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

	// 遍历 rows 并填充 Arrow Builder
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// 将值添加到 Arrow Builder 中
		for i, val := range values {
			switch builder.Field(i).(type) {
			case *array.StringBuilder:
				strValue := fmt.Sprintf("%v", val)
				builder.Field(i).(*array.StringBuilder).Append(strValue)
			case *array.Int64Builder:
				intValue, ok := val.(int64)
				if ok {
					builder.Field(i).(*array.Int64Builder).Append(intValue)
				}
				// 其他类型转换根据实际需求处理
			}
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
	// 处理其他类型
	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType.DatabaseTypeName())
	}
}
