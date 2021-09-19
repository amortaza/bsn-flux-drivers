package stdsql

import (
	"fmt"
	"github.com/amortaza/bsn-flux/normalization_config"
)

type TableCreator struct {
	sqlRunner *SQLRunner
}

func newTableCreator(sqlRunner *SQLRunner) *TableCreator {
	return &TableCreator{sqlRunner: sqlRunner}
}

func (creator *TableCreator) Create(table string) error {
	var sql = creator.generateSQLToCreateTable(table)

	return creator.sqlRunner.Run(sql)
}

func (creator *TableCreator) generateSQLToCreateTable(table string) string {
	return fmt.Sprintf("CREATE TABLE `%s` (`%s` CHAR(32) NOT NULL, PRIMARY KEY (`%s`));", table, normalization_config.PrimaryKey_FieldName, normalization_config.PrimaryKey_FieldName)
}
