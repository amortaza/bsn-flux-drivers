package mysql

import (
	"fmt"
	"github.com/amortaza/bsn/drivers/stdsql"
	"testing"

	"github.com/amortaza/bsn/flux/node"
)

func TestWhenNoWhereClause_ExpectNoError(t *testing.T) {
	s := newSelectCompiler("u_user", nil)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user" {
		t.Error()
	}
}

func TestWhenEqual_ExpectNoError(t *testing.T) {
	equal := node.NewEqual(stdsql.NewNodeCompiler())
	equal.Left = node.NewColumn("name", stdsql.NewNodeCompiler())
	equal.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", equal)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name = 'ace'" {
		fmt.Print(q)
		t.Error()
	}
}

func TestNodeWhenAnd_ExpectNoError(t *testing.T) {
	name := node.NewEqual(stdsql.NewNodeCompiler())
	name.Left = node.NewColumn("name", stdsql.NewNodeCompiler())
	name.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	age := node.NewEqual(stdsql.NewNodeCompiler())
	age.Left = node.NewColumn("age", stdsql.NewNodeCompiler())
	age.Right = node.NewNumber(44, stdsql.NewNodeCompiler())

	and := node.NewAnd(stdsql.NewNodeCompiler())
	and.Left = name
	and.Right = age

	s := newSelectCompiler("u_user", and)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE ( name = 'ace' AND age = 44.000000 )" {
		fmt.Print(q)
		t.Error()
	}
}

func TestNodeWhenAndOr_ExpectNoError(t *testing.T) {
	name := node.NewEqual(stdsql.NewNodeCompiler())
	name.Left = node.NewColumn("name", stdsql.NewNodeCompiler())
	name.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	age := node.NewEqual(stdsql.NewNodeCompiler())
	age.Left = node.NewColumn("age", stdsql.NewNodeCompiler())
	age.Right = node.NewNumber(44, stdsql.NewNodeCompiler())

	and := node.NewAnd(stdsql.NewNodeCompiler())
	and.Left = name
	and.Right = age

	or := node.NewOr(stdsql.NewNodeCompiler())
	or.Left = and
	or.Right = age

	s := newSelectCompiler("u_user", or)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE ( ( name = 'ace' AND age = 44.000000 ) OR age = 44.000000 )" {
		fmt.Print(q)
		t.Error()
	}
}

func TestWhenInStringClause_ExpectNoError(t *testing.T) {
	in := node.NewIn(stdsql.NewNodeCompiler())
	in.Left = node.NewColumn("name", stdsql.NewNodeCompiler())
	in.Right = node.NewStringList([]string{"ace", "clown", "reek"}, stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", in)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name IN [ 'ace', 'clown', 'reek' ]" {
		fmt.Print(q)
		t.Error()
	}
}

func TestWhenInNumberClause_ExpectNoError(t *testing.T) {
	in := node.NewIn(stdsql.NewNodeCompiler())
	in.Left = node.NewColumn("name", stdsql.NewNodeCompiler())
	in.Right = node.NewNumberList([]int{1, 2, 3, 4, 5}, stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", in)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name IN [ 1, 2, 3, 4, 5 ]" {
		fmt.Print(q)
		t.Error()
	}
}

func TestNodeWhenStartsWith_ExpectNoError(t *testing.T) {
	colnode := node.NewColumn("name", stdsql.NewNodeCompiler())
	startsWith := node.NewStartsWith(stdsql.NewNodeCompiler())

	startsWith.Left = colnode
	startsWith.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", startsWith)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name LIKE 'ace%'" {
		fmt.Print(q)
		t.Error()
	}
}

func TestNodeWhenContains_ExpectNoError(t *testing.T) {
	colnode := node.NewColumn("name", stdsql.NewNodeCompiler())
	contains := node.NewContains(stdsql.NewNodeCompiler())

	contains.Left = colnode
	contains.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", contains)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name LIKE '%ace%'" {
		fmt.Print(q)
		t.Error()
	}
}

func TestNodeWhenEndsWith_ExpectNoError(t *testing.T) {
	colnode := node.NewColumn("name", stdsql.NewNodeCompiler())
	endsWith := node.NewEndsWith(stdsql.NewNodeCompiler())

	endsWith.Left = colnode
	endsWith.Right = node.NewString("ace", stdsql.NewNodeCompiler())

	s := newSelectCompiler("u_user", endsWith)
	q, _ := s.Compile()

	if q != "SELECT * FROM u_user WHERE name LIKE '%ace'" {
		fmt.Print(q)
		t.Error()
	}
}
