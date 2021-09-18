package stdsql

import (
	"fmt"
)

type TableDeleter struct {
	sqlRunner *SQLRunner
}

func newTableDeleter(sqlRunner *SQLRunner) *TableDeleter {
	return &TableDeleter{
		sqlRunner: sqlRunner,
	}
}

func (deleter *TableDeleter) Delete(table string) error {
	var sql = deleter.generateSQLToDeleteTable(table)

	return deleter.sqlRunner.Run(sql)
}

func (deleter *TableDeleter) generateSQLToDeleteTable(table string) string {
	return fmt.Sprintf("DROP TABLE `%s`;", table)
}
