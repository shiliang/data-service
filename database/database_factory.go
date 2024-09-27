/*
*

	@author: shiliang
	@date: 2024/9/20
	@note: 获取数据源连接工厂

*
*/
package database

import (
	"database/sql"
	"errors"
	pb "github.com/shiliang/data-service/generated/datasource"
	ida "github.com/shiliang/data-service/generated/ida"
)

// DatabaseFactory creates a database strategy based on the database type
func DatabaseFactory(dbType pb.DataSourceType, info *ida.DBConnInfo) (DatabaseStrategy, error) {
	switch dbType {
	case pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		return NewKingbaseStrategy(info), nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL:
		return NewMySQLStrategy(info), nil
	default:
		return nil, errors.New("unknown database type")
	}
}

func GetDB(dbStrategy DatabaseStrategy) *sql.DB {
	if mysqlStrategy, ok := dbStrategy.(*MySQLStrategy); ok {
		return mysqlStrategy.DB
	} else if kingbaseStrategy, ok := dbStrategy.(*KingbaseStrategy); ok {
		return kingbaseStrategy.DB
	} else {
		return nil
	}
}
