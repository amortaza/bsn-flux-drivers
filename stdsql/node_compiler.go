package stdsql

import (
	"fmt"
	"strconv"

	"github.com/amortaza/bsn-flux/node"
)

type nodeCompiler struct{}

func NewNodeCompiler() *nodeCompiler {
	return &nodeCompiler{}
}

func (compiler *nodeCompiler) AndCompile(and *node.And) (string, error) {
	if and.Left == nil || and.Right == nil {
		return "", fmt.Errorf("both the left and right of an AND expression must have a value")
	}

	leftSQL, err1 := and.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := and.Right.Compile()
	if err2 != nil {
		return "", err2
	}

	return "( " + leftSQL + " AND " + rightSQL + " )", nil
}

func (compiler *nodeCompiler) ColumnCompile(column *node.Column) (string, error) {
	if column.Name == "" {
		return "", fmt.Errorf("column name cannot be blank")
	}

	return column.Name, nil
}

func (compiler *nodeCompiler) ContainsCompile(contains *node.Contains) (string, error) {
	if contains.Left == nil || contains.Right == nil {
		return "", fmt.Errorf("both left and right sides of a CONTAINS expression must have values")
	}

	leftSQL, err1 := contains.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := contains.Right.Compile()
	if err2 != nil {
		return "", err1
	}

	not := ""

	if contains.Not {
		not = " NOT"
	}

	// rightSQL must have been a string, and strings are quoted
	// we must de-quote since we are adding our own quotes for like
	index := len(rightSQL) - 1
	rightSQL = rightSQL[1:index]

	return leftSQL + not + " LIKE '%" + rightSQL + "%'", nil
}

func (compiler *nodeCompiler) EndsWithCompile(endsWith *node.EndsWith) (string, error) {
	if endsWith.Left == nil || endsWith.Right == nil {
		return "", fmt.Errorf("both left and right sides of an ENDS WITH expression must have values")
	}

	leftSQL, err1 := endsWith.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := endsWith.Right.Compile()
	if err2 != nil {
		return "", err2
	}

	not := ""

	if endsWith.Not {
		not = " NOT"
	}

	// rightSQL must have been a string, and strings are quoted
	// we must de-quote since we are adding our own quotes for like
	index := len(rightSQL) - 1
	rightSQL = rightSQL[1:index]

	return leftSQL + not + " LIKE '%" + rightSQL + "'", nil
}

func (compiler *nodeCompiler) EqualCompile(equal *node.Equals) (string, error) {
	if equal.Left == nil || equal.Right == nil {
		return "", fmt.Errorf("both left and right sides of an EQUAL expression must have values")
	}

	leftSQL, err1 := equal.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := equal.Right.Compile()
	if err2 != nil {
		return "", err1
	}

	if equal.Not {
		return leftSQL + " != " + rightSQL, nil
	}

	return leftSQL + " = " + rightSQL, nil
}

func (compiler *nodeCompiler) GreaterThanCompile(greaterThan *node.GreaterThan) (string, error) {
	if greaterThan.Left == nil || greaterThan.Right == nil {
		return "", fmt.Errorf("both left and right sides of a GREATER THAN expression must have values")
	}

	leftSQL, err1 := greaterThan.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := greaterThan.Right.Compile()
	if err2 != nil {
		return "", err1
	}

	op := " > "

	if greaterThan.OrEquals {
		op = " >= "
	}

	return leftSQL + op + rightSQL, nil
}

func (compiler *nodeCompiler) InCompile(inNode *node.In) (string, error) {
	if inNode.Left == nil || inNode.Right == nil {
		return "", fmt.Errorf("both left and right sides of an IN expression must have values")
	}

	leftSQL, err1 := inNode.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := inNode.Right.Compile()
	if err2 != nil {
		return "", err1
	}

	if inNode.Not {
		return leftSQL + " NOT IN " + rightSQL, nil
	}

	return leftSQL + " IN " + rightSQL, nil
}

func (compiler *nodeCompiler) IsNullCompile(isNull *node.IsNull) (string, error) {
	sql, err := isNull.ColumnNode.Compile()
	if err != nil {
		return "", err
	}

	if isNull.Not {
		return sql + " IS NOT NULL", nil
	}

	return sql + " IS NULL", nil
}

func (compiler *nodeCompiler) LessThanCompile(lessThan *node.LessThan) (string, error) {
	if lessThan.Left == nil || lessThan.Right == nil {
		return "", fmt.Errorf("both left and right sides of a LESS THAN expression must have values")
	}

	leftSQL, err1 := lessThan.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := lessThan.Right.Compile()
	if err2 != nil {
		return "", err1
	}

	op := " < "

	if lessThan.OrEquals {
		op = " <= "
	}

	return leftSQL + op + rightSQL, nil
}

func (compiler *nodeCompiler) NotCompile(notNode *node.Not) (string, error) {
	if notNode.Left == nil {
		return "", fmt.Errorf("left side of a NOT expression must have a value")
	}

	leftSQL, err := notNode.Left.Compile()

	if err != nil {
		return "", err
	}

	return "NOT ( " + leftSQL + " )", nil
}

func (compiler *nodeCompiler) NumberCompile(number *node.Number) (string, error) {
	if number.Text == "" {
		return "", fmt.Errorf("number literal cannot be blank")
	}

	return number.Text, nil
}

func (compiler *nodeCompiler) NumberListCompile(numberList *node.NumberList) (string, error) {
	sql := "[ "

	last := len(numberList.Numbers) - 1

	for i, e := range numberList.Numbers {

		s := strconv.Itoa(e)

		if i == last {
			sql += s
		} else {
			sql += s + ", "
		}
	}

	return sql + " ]", nil
}

func (compiler *nodeCompiler) OrCompile(orNode *node.Or) (string, error) {
	if orNode.Left == nil || orNode.Right == nil {
		return "", fmt.Errorf("both left and right sides of an OR expression must have values")
	}

	leftSQL, err1 := orNode.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := orNode.Right.Compile()
	if err2 != nil {
		return "", err2
	}

	return "( " + leftSQL + " OR " + rightSQL + " )", nil
}

func (compiler *nodeCompiler) StartsWithCompile(startsWith *node.StartsWith) (string, error) {
	if startsWith.Left == nil || startsWith.Right == nil {
		return "", fmt.Errorf("both left and right sides of a STARTS WITH expression must have values")
	}

	leftSQL, err1 := startsWith.Left.Compile()
	if err1 != nil {
		return "", err1
	}

	rightSQL, err2 := startsWith.Right.Compile()
	if err2 != nil {
		return "", err2
	}

	not := ""

	if startsWith.Not {
		not = " NOT"
	}

	// rightSQL must have been a string, and strings are quoted
	// we must de-quote since we are adding our own quotes for like
	index := len(rightSQL) - 1
	rightSQL = rightSQL[1:index]

	return leftSQL + not + " LIKE '" + rightSQL + "%'", nil
}

func (compiler *nodeCompiler) StringCompile(stringNode *node.String) (string, error) {
	return "'" + stringNode.Text + "'", nil
}

func (compiler *nodeCompiler) StringListCompile(stringList *node.StringList) (string, error) {
	sql := "[ "

	last := len(stringList.Strings) - 1

	for i, s := range stringList.Strings {

		if i == last {
			sql += fmt.Sprintf("'%v'", s)
		} else {
			sql += fmt.Sprintf("'%v', ", s)
		}
	}

	return sql + " ]", nil
}
