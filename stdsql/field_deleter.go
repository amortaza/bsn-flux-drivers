package stdsql

import (
	"fmt"
)

type fieldDeleter struct {
	sqlRunner *SQLRunner
}

func newFieldDeleter(sqlRunner *SQLRunner) *fieldDeleter {
	return &fieldDeleter{
		sqlRunner: sqlRunner,
	}
}

func (deleter *fieldDeleter) Delete(table string, fieldname string) error {
	sql := deleter.generateSQLToDeleteField(table, fieldname)

	return deleter.sqlRunner.Run(sql)
}

func (deleter *fieldDeleter) generateSQLToDeleteField(table string, fieldname string) string {
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP `%s`;", table, fieldname)

	return sql
}
