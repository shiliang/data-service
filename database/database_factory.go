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
	ida "data-service/generated/ida"
	"errors"
)

// DatabaseFactory creates a database strategy based on the database type
func DatabaseFactory(dbType pb.DataSourceType, info *ida.DBConnInfo) (DatabaseStrategy, error) {
	switch dbType {
	case pb.DataSourceType_DATA_SOURCE_TYPE_KINGBASE:
		return &KingbaseStrategy{host, port, database, username, password}, nil
	case pb.DataSourceType_DATA_SOURCE_TYPE_MYSQL:
		return NewMySQLStrategy(info), nil
	default:
		return nil, errors.New("unknown database type")
	}
}
