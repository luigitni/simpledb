package record

import (
	"io"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

type BasicUpdatePlanner struct {
	mdm *MetadataManager
}

func newBasicUpdatePlanner(mdm *MetadataManager) BasicUpdatePlanner {
	return BasicUpdatePlanner{
		mdm: mdm,
	}
}

// iterateAndExecute scans through the records that satisfy predicate and executes exec on each.
func (bup BasicUpdatePlanner) iterateAndExecute(x tx.Transaction, tableName string, predicate sql.Predicate, exec func(us UpdateScan) error) (int, error) {
	var p Plan

	p, err := newTablePlan(x, tableName, bup.mdm)
	if err != nil {
		return 0, err
	}

	p = newSelectPlan(p, predicate)
	s, err := p.Open()
	if err != nil {
		return 0, err
	}

	us := s.(UpdateScan)
	defer us.Close()

	c := 0
	for {

		err := us.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return c, err
		}

		if err := exec(us); err != nil {
			return c, err
		}

		c++
	}

	return c, nil
}

func (bup BasicUpdatePlanner) executeUpdate(data sql.UpdateCommand, x tx.Transaction) (int, error) {
	exec := func(us UpdateScan) error {
		val, err := data.NewValue.Evaluate(us)
		if err != nil {
			return err
		}

		if err := us.SetVal(data.Field, val); err != nil {
			return err
		}

		return nil
	}

	return bup.iterateAndExecute(x, data.TableName, data.Predicate, exec)
}

func (bup BasicUpdatePlanner) executeDelete(data sql.DeleteCommand, x tx.Transaction) (int, error) {
	exec := func(us UpdateScan) error {
		return us.Delete()
	}

	return bup.iterateAndExecute(x, data.TableName, data.Predicate, exec)
}

func (bup BasicUpdatePlanner) executeInsert(data sql.InsertCommand, x tx.Transaction) (int, error) {
	p, err := newTablePlan(x, data.TableName, bup.mdm)
	if err != nil {
		return 0, err
	}

	s, err := p.Open()
	if err != nil {
		return 0, err
	}

	us := s.(UpdateScan)
	defer us.Close()

	if err := us.Insert(); err != nil {
		return 0, err
	}

	for i, fieldName := range data.Fields {
		v := data.Values[i]
		if err := us.SetVal(fieldName, v); err != nil {
			return 0, err
		}
	}

	return 1, nil
}

func (bup BasicUpdatePlanner) executeCreateTableFromSchema(tableName string, schema Schema, x tx.Transaction) (int, error) {
	err := bup.mdm.createTable(tableName, schema, x)
	return 0, err
}

func (bup BasicUpdatePlanner) executeCreateTable(data sql.CreateTableCommand, x tx.Transaction) (int, error) {
	schema := newSchema()
	for _, field := range data.Fields {
		schema.addField(field.Name, field.Type, field.Len)
	}

	err := bup.mdm.createTable(data.TableName, schema, x)
	return 0, err
}

func (bup BasicUpdatePlanner) executeCreateView(data sql.CreateViewCommand, x tx.Transaction) (int, error) {
	err := bup.mdm.createView(data.ViewName, data.Definition(), x)
	return 0, err
}

func (bup BasicUpdatePlanner) executeCreateIndex(data sql.CreateIndexCommand, x tx.Transaction) (int, error) {
	err := bup.mdm.createIndex(x, data.IndexName, data.TableName, data.TargetField)
	return 0, err
}
