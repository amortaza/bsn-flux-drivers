package stdsql

import (
	"database/sql"
	"fmt"
	"github.com/amortaza/bsn/logger"
	"time"

	// need to have driver available otherwise sql.Open() fails
	_ "github.com/go-sql-driver/mysql"
)

type SQLRunner struct {
	db       *sql.DB
	lastPing time.Time

	driverName, dataSourceName string
}

func NewSQLRunner(driverName string, dataSourceName string) *SQLRunner {
	return &SQLRunner{
		driverName:     driverName,
		dataSourceName: dataSourceName,
	}
}

func (runner *SQLRunner) Run(sql string) error {
	if err := runner.ping(); err != nil {
		return err
	}

	logger.Log(sql, logger.SQL)

	_, err := runner.db.Exec(sql)

	return err
}

func (runner *SQLRunner) Query(sql string) (*sql.Rows, error) {
	if err := runner.ping(); err != nil {
		return nil, err
	}

	return runner.db.Query(sql)
}

func (runner *SQLRunner) ping() error {
	if runner.db == nil {
		var err error

		runner.db, err = sql.Open(runner.driverName, runner.dataSourceName)
		if err != nil {
			return fmt.Errorf("%v", err)
		}

		runner.lastPing = time.Now()

		return nil
	}

	if time.Since(runner.lastPing) < 1*time.Minute {
		return nil
	}

	runner.lastPing = time.Now()

	err := runner.db.Ping()

	return err
}
