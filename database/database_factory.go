/*
*

	@author: shiliang
	@date: 2024/9/20
	@note:

*
*/
package database

import (
	pb "data-service/generated/datasource"
	"errors"
)

// DatabaseFactory creates a database strategy based on the database type
func DatabaseFactory(dbType pb.DataSourceType, host string, port int, database, username, password string) (DatabaseStrategy, error) {
	switch dbType {
	case pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		return &KingbaseStrategy{host, port, database, username, password}, nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL:
		return &MySQLStrategy{host, port, database, username, password}, nil
	default:
		return nil, errors.New("unknown database type")
	}
}
