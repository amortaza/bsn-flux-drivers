package mysql

import (
	"github.com/amortaza/bsn/drivers/stdsql"
	"github.com/amortaza/bsn/flux"
	"github.com/amortaza/bsn/flux/node"
)

type crud struct {
	querier  *stdsql.RowQuerier
	compiler node.Compiler
}

func NewCRUD() flux.CRUD {
	return &crud{
		compiler: stdsql.NewNodeCompiler(),
	}
}

func (crud *crud) Compiler() node.Compiler {
	return crud.compiler
}

func (crud *crud) Query(table string, root node.Node) error {
	selectCompiler := newSelectCompiler(table, root)

	crud.querier = stdsql.NewQuerier(mysqlRunner, selectCompiler)

	return crud.querier.Query()
}

// returns nil if there are no records left
func (crud *crud) Next() (*flux.RecordMap, error) {
	return crud.querier.Next()
}

func (crud *crud) Create(table string, values *flux.RecordMap) (string, error) {
	return stdsql.NewRowInserter(mysqlRunner).Insert(table, values)
}

func (crud *crud) Update(table string, id string, values *flux.RecordMap) error {
	return stdsql.NewRowUpdater(mysqlRunner).Update(table, id, values)
}

func (crud *crud) Delete(table string, id string) error {
	return stdsql.NewRowDeleter(mysqlRunner).Delete(table, id)
}
