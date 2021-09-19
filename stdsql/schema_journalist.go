package stdsql

import (
	"github.com/amortaza/bsn-flux"
	"github.com/amortaza/bsn-flux/query"
	"github.com/amortaza/bsn-flux/relation"
)

type schemaJournalist struct {
	crud flux.CRUD
}

func NewSchemaJournalist(crud flux.CRUD) flux.SchemaJournalist {
	return &schemaJournalist{crud: crud}
}

func (journalist *schemaJournalist) CreateRelation(collectionName string) error {
	recordmap := flux.NewRecordMap()

	recordmap.Put("x_collection", collectionName)
	recordmap.Put("x_field", "x_pk")
	recordmap.Put("x_field_type", string(relation.String))

	_, err := journalist.crud.Create("x_dictionary", recordmap)

	return err
}

func (journalist *schemaJournalist) DeleteRelation(collectionName string) error {
	record := flux.NewRecord("x_dictionary", journalist.crud)

	_ = record.Add("x_collection", query.Equals, collectionName)

	_ = record.Query()

	for {
		has, _ := record.Next()

		if !has {
			break
		}

		id, _ := record.Get("x_pk")
		_ = journalist.crud.Delete("x_dictionary", id)
	}

	return nil
}

func (journalist *schemaJournalist) CreateField(colletionName string, field *relation.Field) error {
	recordmap := flux.NewRecordMap()

	recordmap.Put("x_collection", colletionName)
	recordmap.Put("x_field", field.Name)
	recordmap.Put("x_field_type", string(field.Type))

	_, err := journalist.crud.Create("x_dictionary", recordmap)

	return err
}

func (journalist *schemaJournalist) DeleteField(collectionName string, fieldname string) error {
	record := flux.NewRecord("x_dictionary", journalist.crud)

	_ = record.Add("x_collection", query.Equals, collectionName)
	_ = record.Add("x_field", query.Equals, fieldname)

	_ = record.Query()

	for {
		has, _ := record.Next()

		if !has {
			break
		}

		id, _ := record.Get("x_pk")
		_ = journalist.crud.Delete("x_dictionary", id)
	}

	return nil
}
