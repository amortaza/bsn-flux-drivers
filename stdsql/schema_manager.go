package stdsql

import (
	"github.com/amortaza/bsn/flux"
	"github.com/amortaza/bsn/flux/relation"
)

type schemaManager struct {
	tableCreator *TableCreator
	tableDeleter *TableDeleter
	fieldCreator *fieldCreator
	fieldDeleter *fieldDeleter

	journalist flux.SchemaJournalist
}

func CreateAndJournalCollection(collection *relation.Relation, crud flux.CRUD, sqlrunner *SQLRunner) error {
	journalist := NewSchemaJournalist(crud)

	schema := NewSchemaManager(sqlrunner, journalist)

	if err := schema.CreateRelation(collection.Name()); err != nil {
		return err
	}

	for _, field := range collection.Fields() {
		if err := schema.CreateField(collection.Name(), field); err != nil {
			return err
		}
	}

	return nil
}

func NewSchemaManager(sqlRunner *SQLRunner, journalist flux.SchemaJournalist) flux.SchemaManager {
	return &schemaManager{
		tableCreator: newTableCreator(sqlRunner),
		tableDeleter: newTableDeleter(sqlRunner),
		fieldCreator: newFieldCreator(sqlRunner),
		fieldDeleter: newFieldDeleter(sqlRunner),
		journalist:   journalist,
	}
}

func (schema *schemaManager) CreateRelation(collectionName string) error {
	_ = schema.journalist.CreateRelation(collectionName)

	return schema.tableCreator.Create(collectionName)
}

func (schema *schemaManager) DeleteRelation(collectionName string) error {
	_ = schema.journalist.DeleteRelation(collectionName)

	return schema.tableDeleter.Delete(collectionName)
}

func (schema *schemaManager) CreateField(collectionName string, field *relation.Field) error {
	_ = schema.journalist.CreateField(collectionName, field)

	return schema.fieldCreator.Create(collectionName, field)
}

func (schema *schemaManager) DeleteField(collectionName string, fieldname string) error {
	_ = schema.journalist.DeleteField(collectionName, fieldname)

	return schema.fieldDeleter.Delete(collectionName, fieldname)
}
