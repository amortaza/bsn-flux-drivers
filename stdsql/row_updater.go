package stdsql

import (
	"fmt"
	"github.com/amortaza/bsn/flux"
	"github.com/amortaza/bsn/flux/normalization"
)

type rowUpdater struct {
	sqlRunner *SQLRunner
}

func NewRowUpdater(sqlRunner *SQLRunner) *rowUpdater {
	return &rowUpdater{
		sqlRunner: sqlRunner,
	}
}

func (updater *rowUpdater) Update(table string, pk string, data *flux.RecordMap) error {
	sql := updater.generateSQL(table, pk, data)

	return updater.sqlRunner.Run(sql)
}

func (updater *rowUpdater) generateSQL(table string, pk string, data *flux.RecordMap) string {
	sql := fmt.Sprintf("UPDATE `%s` SET ", table)
	first := true

	for key, value := range data.Data {

		// skip primary key
		if key == normalization.PrimaryKeyFieldname {
			continue
		}

		// add commas (,)
		if first {
			first = false
		} else {
			sql = fmt.Sprintf("%s, ", sql)
		}

		sql = fmt.Sprintf("%s `%s` = %s", sql, key, updater.valueToSQL(value))
	}

	return fmt.Sprintf("%s WHERE %s ='%s';", sql, normalization.PrimaryKeyFieldname, pk)
}

func (updater *rowUpdater) valueToSQL(value interface{}) string {
	sql := ""

	if stringValue, ok := value.(string); ok {
		sql = fmt.Sprintf("'%s'", stringValue)
	} else {
		sql = fmt.Sprintf("%v", value)
	}

	return sql
}
