/*
*

	@author: shiliang
	@date: 2024/9/30
	@note:

*
*/
package utils

import (
	"github.com/apache/arrow/go/v15/arrow"
	"testing"
)

func TestGenerateInsertSQL(t *testing.T) {
	type args struct {
		tableName string
		rowData   []interface{}
		schema    *arrow.Schema
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "successful insert",
			args: args{
				tableName: "my_table",
				rowData: []interface{}{
					1, "Alice", 30,
					2, "Bob", 25,
				},
				schema: arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
				}, nil),
			},
			want:    "INSERT INTO my_table (id, name, age) VALUES (?, ?, ?), (?, ?, ?)",
			wantErr: false,
		},
		{
			name: "row data length mismatch",
			args: args{
				tableName: "my_table",
				rowData: []interface{}{
					1, "Alice", 30,
					2, "Bob", // 这里缺少 age 字段
				},
				schema: arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
				}, nil),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "empty row data",
			args: args{
				tableName: "my_table",
				rowData:   []interface{}{},
				schema: arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
				}, nil),
			},
			want:    "INSERT INTO my_table (id, name, age) VALUES ",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateInsertSQL(tt.args.tableName, tt.args.rowData, tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateInsertSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GenerateInsertSQL() got = %v, want %v", got, tt.want)
			}
		})
	}
}
