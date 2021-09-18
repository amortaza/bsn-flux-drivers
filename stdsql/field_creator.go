package stdsql

import (
	"fmt"
	"github.com/amortaza/bsn/flux/relation"
)

type fieldCreator struct {
	sqlRunner *SQLRunner
}

func newFieldCreator(sqlRunner *SQLRunner) *fieldCreator {
	return &fieldCreator{
		sqlRunner: sqlRunner,
	}
}

func (creator *fieldCreator) Create(table string, field *relation.Field) error {
	sql, err := creator.generateSQLToCreateField(table, field)
	if err != nil {
		return err
	}

	return creator.sqlRunner.Run(sql)
}

func (creator *fieldCreator) generateSQLToCreateField(table string, field *relation.Field) (string, error) {
	sqlType, err := creator.fieldTypeToSQLType(field.Type)
	if err != nil {
		return "", err
	}

	var defaultValue string
	defaultValue, err = creator.fieldTypeToDefaultValue(field.Type)
	if err != nil {
		return "", err
	}

	sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s NULL DEFAULT %s;", table, field.Name, sqlType, defaultValue)

	return sql, nil
}

func (creator *fieldCreator) fieldTypeToDefaultValue(fieldType relation.FieldType) (string, error) {
	if fieldType == relation.String {
		return "NULL", nil
	}

	if fieldType == relation.Bool {
		return "0", nil
	}

	if fieldType == relation.Number {
		return "0", nil
	}

	return "", fmt.Errorf("unrecognized fieldtype `%s`", fieldType)
}

func (creator *fieldCreator) fieldTypeToSQLType(fieldType relation.FieldType) (string, error) {
	if fieldType == relation.String {
		return "VARCHAR(255)", nil
	}

	if fieldType == relation.Bool {
		return "TINYINT", nil
	}

	if fieldType == relation.Number {
		return "DECIMAL(10,5)", nil
	}

	return "", fmt.Errorf("unrecognized fieldtype %s", fieldType)
}
