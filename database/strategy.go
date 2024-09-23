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
)

type DatabaseStrategy interface {
	ConnectToDB() error

	// 执行select
	Query(sqlQuery string, args ...interface{}) (*sql.Rows, error)
	// 执行insert

	// 关闭连接
	Close() error

	GetJdbcUrl() (string, error)
}
