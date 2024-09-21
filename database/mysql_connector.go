/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接mysql

*
*/
package database

import "fmt"

type MySQLStrategy struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

func (m MySQLStrategy) GetJdbcUrl() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.Username, m.Password, m.Host, m.Port, m.Database)
}

func (m MySQLStrategy) GetUser() string {
	return m.Username
}

func (m MySQLStrategy) GetPassword() string {
	return m.Password
}

func (m MySQLStrategy) GetDriver() string {
	return "com.mysql.cj.jdbc.Driver"
}
