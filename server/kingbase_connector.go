/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 连接kingbase

*
*/
package main

import "fmt"

// KingbaseStrategy is a struct that implements DatabaseStrategy for Kingbase database
type KingbaseStrategy struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

func (k KingbaseStrategy) GetJdbcUrl() string {
	return fmt.Sprintf("jdbc:kingbase8://%s:%d/%s", k.Host, k.Port, k.Database)
}

func (k KingbaseStrategy) GetUser() string {
	return k.Username
}

func (k KingbaseStrategy) GetPassword() string {
	return k.Password
}

func (k KingbaseStrategy) GetDriver() string {
	return "com.kingbase8.Driver"
}
