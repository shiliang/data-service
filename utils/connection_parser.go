package utils

import (
	"errors"
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
