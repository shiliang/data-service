/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接kingbase

*
*/
package database

import (
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	ds "github.com/shiliang/data-service/generated/datasource"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
)

// KingbaseStrategy is a struct that implements DatabaseStrategy for Kingbase database
type KingbaseStrategy struct {
	info   *ds.ConnectionInfo
	DB     *sql.DB
	logger *zap.SugaredLogger
}

func (k *KingbaseStrategy) RowsToArrowBatch(rows *sql.Rows) (arrow.Record, error) {
	//TODO implement me
	panic("implement me")
}

func (k *KingbaseStrategy) ConnectToDBWithPass(info *ds.ConnectionInfo) error {
	// 构造 Kingbase DSN 数据源名称，包含用户名、密码和连接信息
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", info.Host, info.Port, info.User, info.Password, info.DbName)

	// 打开数据库连接
	db, err := sql.Open("kingbase", dsn) // 使用 Kingbase 驱动
	if err != nil {
		k.logger.Errorf("Failed to connect to Kingbase: %v", err)
		return fmt.Errorf("failed to connect to Kingbase: %v", err)
	}

	// 成功连接后，保存数据库实例
	k.DB = db
	k.logger.Info("Successfully connected to Kingbase with username and password")
	return nil
}

func NewKingbaseStrategy(info *ds.ConnectionInfo) *KingbaseStrategy {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	return &KingbaseStrategy{
		info:   info,
		logger: sugar,
	}
}

// 将证书字符串写入临时文件
func writeCertToTempFile(certContent string) (string, error) {
	tempFile, err := ioutil.TempFile("", "tls-cert-*.pem")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %v", err)
	}

	// 将证书内容写入文件
	if _, err := tempFile.Write([]byte(certContent)); err != nil {
		return "", fmt.Errorf("failed to write cert to temp file: %v", err)
	}

	if err := tempFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %v", err)
	}

	// 返回临时文件路径
	return tempFile.Name(), nil
}

func (k *KingbaseStrategy) ConnectToDB() error {
	// 将证书字符串写入临时文件
	certPath, err := writeCertToTempFile(k.info.TlsCert)
	if err != nil {
		k.logger.Fatalf("Failed to write cert to temp file: %v", err)
		return fmt.Errorf("failed to write cert to temp file: %v", err)
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {

		}
	}(certPath) // 程序结束后删除临时文件

	// 使用 PostgreSQL 驱动的标准 DSN 格式，并通过 sslrootcert 引用证书文件
	sslmode := "verify-full"
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&sslrootcert=%s",
		k.info.User, k.info.Host, k.info.Port, k.info.DbName, sslmode, certPath)

	// 打开数据库连接
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		k.logger.Errorf("Failed to connect to Kingbase: %v", err)
		return fmt.Errorf("failed to connect to Kingbase: %v", err)
	}

	// 检查连接是否成功
	err = db.Ping()
	if err != nil {
		k.logger.Errorf("Failed to ping Kingbase: %v", err)
		return fmt.Errorf("failed to ping Kingbase: %v", err)
	}

	k.DB = db
	k.logger.Info("Successfully connected to Kingbase")
	return nil
}

func (k *KingbaseStrategy) Query(sqlQuery string, args ...interface{}) (*sql.Rows, error) {
	k.logger.Infof("Executing query: %s with args: %v\n", sqlQuery, args)
	// 执行查询，args 用于绑定 SQL 查询中的占位符
	rows, err := k.DB.Query(sqlQuery, args...)
	if err != nil {
		k.logger.Errorf("Query failed: %v\n", err)
		return nil, err
	}

	return rows, nil

}

func (k *KingbaseStrategy) Close() error {
	// 检查数据库连接是否已经初始化
	if k.DB != nil {
		err := k.DB.Close()
		if err != nil {
			k.logger.Errorf("Failed to close Kingbase connection: %v", err)
			return err
		}
		k.logger.Info("Kingbase connection closed successfully")
		return nil
	}
	// 如果数据库连接未初始化，直接返回 nil
	k.logger.Warn("Attempted to close a non-initialized DB connection")
	return nil

}

func (k *KingbaseStrategy) GetJdbcUrl() string {
	// 构建 JDBC URL
	jdbcUrl := fmt.Sprintf(
		"jdbc:kingbase8://%s:%d/%s?user=%s&password=%s",
		k.info.Host,
		k.info.Port,
		k.info.DbName,
		k.info.User,     // 添加用户名
		k.info.Password, // 添加密码
	)
	return jdbcUrl
}
