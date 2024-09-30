/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接数据库接口

*
*/
package database

import (
	"database/sql"
	"github.com/apache/arrow/go/v15/arrow"
	pb "github.com/shiliang/data-service/generated/datasource"
)

type DatabaseStrategy interface {
	ConnectToDB() error

	// 使用密码连接数据库
	ConnectToDBWithPass(info *pb.ConnectionInfo) error

	/* 执行select，args为sql查询中的占位符
	sqlQuery := "SELECT * FROM users WHERE age > ? AND city = ?"
	rows, err := mySQLStrategy.Query(sqlQuery, 25, "New York")
	*/
	Query(sqlQuery string, args ...interface{}) (*sql.Rows, error)
	// 执行insert

	// 关闭连接
	Close() error

	GetJdbcUrl() (string, error)

	RowsToArrowBatch(rows *sql.Rows) (arrow.Record, error)
}
