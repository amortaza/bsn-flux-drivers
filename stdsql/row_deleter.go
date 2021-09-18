package stdsql

import (
	"fmt"
	"github.com/amortaza/bsn/flux/normalization"
)

type rowDeleter struct {
	sqlRunner *SQLRunner
}

func NewRowDeleter(sqlRunner *SQLRunner) *rowDeleter {
	return &rowDeleter{
		sqlRunner: sqlRunner,
	}
}

func (deleter *rowDeleter) Delete(table string, pk string) error {
	sql := deleter.generateSQL(table, pk)

	return deleter.sqlRunner.Run(sql)
}

func (deleter *rowDeleter) generateSQL(table string, pk string) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s = '%s';", table, normalization.PrimaryKeyFieldname, pk)
}
