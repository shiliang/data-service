/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接数据库接口

*
*/
package database

type DatabaseStrategy interface {
	GetJdbcUrl() string
	GetUser() string
	GetPassword() string
	GetDriver() string
}
