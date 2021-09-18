package mysql

import (
	"github.com/amortaza/bsn/flux/node"
)

type selectCompiler struct {
	From  string
	Where node.Node
}

func newSelectCompiler(table string, where node.Node) *selectCompiler {
	s := &selectCompiler{}

	s.From = table
	s.Where = where

	return s
}

func (s *selectCompiler) Compile() (string, error) {
	q := "SELECT * FROM " + s.From

	if s.Where == nil {
		return q, nil
	}

	sql, err := s.Where.Compile()
	if err != nil {
		return "", err
	}

	if sql != "" {
		q += " WHERE " + sql
	}

	return q, nil
}
