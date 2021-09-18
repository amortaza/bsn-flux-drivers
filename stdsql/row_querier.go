package stdsql

import (
	"database/sql"
	"fmt"
	"github.com/amortaza/bsn/flux"
)

type RowQuerier struct {
	rows    *sql.Rows
	columns []string

	sqlRunner      *SQLRunner
	selectCompiler SelectCompiler
}

func NewQuerier(sqlRunner *SQLRunner, selectCompiler SelectCompiler) *RowQuerier {
	return &RowQuerier{
		sqlRunner:      sqlRunner,
		selectCompiler: selectCompiler,
	}
}

func (query *RowQuerier) Query() error {
	sqlstr, err := query.selectCompiler.Compile()
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	query.rows, err = query.sqlRunner.Query(sqlstr)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	query.columns, err = query.rows.Columns()
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}

// returns nil if there is no records left
func (query *RowQuerier) Next() (*flux.RecordMap, error) {
	has := query.rows.Next()

	if !has {
		return nil, nil
	}

	columns := make([]interface{}, len(query.columns))
	columnPointers := make([]interface{}, len(query.columns))

	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	if err := query.rows.Scan(columnPointers...); err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	values := flux.NewRecordMap()

	for i, name := range query.columns {
		val := columnPointers[i].(*interface{})
		values.Put(name, *val)
	}

	return values, nil
}
