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
	pb "data-service/generated/ida"
	"database/sql"
	"fmt"
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
