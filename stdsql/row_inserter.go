package stdsql

import (
	"fmt"
	"github.com/amortaza/bsn/flux"
	"github.com/amortaza/bsn/flux/normalization"
	"github.com/amortaza/bsn/flux/utils"
)

type RowInserter struct {
	sqlRunner *SQLRunner
}

func NewRowInserter(sqlRunner *SQLRunner) *RowInserter {
	return &RowInserter{
		sqlRunner,
	}
}

func (inserter *RowInserter) Insert(table string, values *flux.RecordMap) (string, error) {
	newId := utils.NewUUID()

	sql := inserter.generateSQL(table, newId, values)

	return newId, inserter.sqlRunner.Run(sql)
}

func (inserter *RowInserter) generateSQL(table string, newId string, values *flux.RecordMap) string {
	columnsSQL := "`" + normalization.PrimaryKeyFieldname + "`"
	valuesSQL := fmt.Sprintf("'%s'", newId)

	for column, value := range values.Data {
		sqlValue := inserter.valueToSQL(value)

		columnsSQL = fmt.Sprintf("%s, `%s`", columnsSQL, column)
		valuesSQL = fmt.Sprintf("%s, %s", valuesSQL, sqlValue)
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES(%s);", table, columnsSQL, valuesSQL)
}

func (inserter *RowInserter) valueToSQL(value interface{}) string {
	sql := ""

	if stringValue, ok := value.(string); ok {
		sql = fmt.Sprintf("'%s'", stringValue)
	} else {
		sql = fmt.Sprintf("%v", value)
	}

	return sql
}
