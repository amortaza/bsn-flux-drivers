package mysql

import "github.com/amortaza/bsn-flux-drivers/stdsql"

// use GetSQLRunner() to get this global variable
var mysqlRunner = stdsql.NewSQLRunner("mysql", "clown:1844@/bsn")

func GetSQLRunner() *stdsql.SQLRunner {
	return mysqlRunner
}
