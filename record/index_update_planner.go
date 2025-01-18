package record

import (
	"io"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

var _ UpdatePlanner = &IndexUpdatePlanner{}

type IndexUpdatePlanner struct {
	mdm *MetadataManager
}

func newIndexUpdatePlanner(mdm *MetadataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{
		mdm: mdm,
	}
}

func (planner *IndexUpdatePlanner) executeInsert(data sql.InsertCommand, x tx.Transaction) (int, error) {
	plan, err := newTablePlan(x, data.TableName, planner.mdm)
	if err != nil {
		return 0, err
	}

	schema := plan.Schema()

	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}

	us := scan.(UpdateScan)
	defer us.Close()

	var size types.Offset = 0
	for i, v := range data.Values {
		// todo: check if the value of type varlena needs to be toasted.
		f := data.Fields[i]

		t := schema.ftype(f)
		size += v.Size(t)
	}

	if err := us.Insert(size); err != nil {
		return 0, err
	}

	rid := us.GetRID()

	ii, err := planner.mdm.indexInfo(x, data.TableName)
	if err != nil {
		return 0, err
	}

	for i, field := range data.Fields {
		val := data.Values[i]
		if err := us.SetVal(field, val); err != nil {
			return 0, err
		}

		// check if the field is indexed. If it is, save it
		info, ok := ii[field]
		if !ok {
			continue
		}

		idx := info.Open()
		defer idx.Close()
		if err := idx.Insert(val, rid); err != nil {
			return 0, err
		}
	}

	return 1, nil
}

func (planner *IndexUpdatePlanner) executeUpdate(data sql.UpdateCommand, x tx.Transaction) (int, error) {
	plan, err := newTablePlan(x, data.TableName, planner.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan := newSelectPlan(plan, data.Predicate)

	s, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}

	updateScan := s.(UpdateScan)
	defer updateScan.Close()

	ii, err := planner.mdm.indexInfo(x, data.TableName)
	if err != nil {
		return 0, err
	}

	updatedRows := 0
	schema := selectPlan.Schema()

	type fieldValue struct {
		field string
		value types.Value
	}

	entryFields := make([]fieldValue, len(schema.fields))

	for {
		err := updateScan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return updatedRows, err
		}

		var size types.Offset = 0

		for _, fieldName := range schema.fields {
			val, err := updateScan.Val(fieldName)
			if err != nil {
				return updatedRows, err
			}

			idx := schema.info[fieldName].Index
			entryFields[idx] = fieldValue{fieldName, val}

			t := schema.ftype(fieldName)

			size += val.Size(t)
		}

		for _, f := range data.Fields {
			val, err := f.NewValue.Evaluate(updateScan)
			if err != nil {
				return updatedRows, err
			}

			idx := schema.info[f.Field].Index

			old := entryFields[idx]

			t := schema.ftype(f.Field)
			size -= old.value.Size(t)

			entryFields[idx] = fieldValue{f.Field, val}
			size += val.Size(t)
		}

		oldRid := updateScan.GetRID()

		if err := updateScan.Update(size); err != nil {
			return updatedRows, err
		}

		newRid := updateScan.GetRID()

		for _, fv := range entryFields {
			if err := updateScan.SetVal(fv.field, fv.value); err != nil {
				return updatedRows, err
			}

			// check if the field is indexed. If it is, save it
			info, ok := ii[fv.field]
			if !ok {
				continue
			}

			idx := info.Open()
			defer idx.Close()

			if err := idx.Delete(fv.value, oldRid); err != nil {
				return updatedRows, err
			}

			if err := idx.Insert(fv.value, newRid); err != nil {
				return updatedRows, err
			}
		}

		updateScan.MoveToRID(oldRid)

		updatedRows++
	}

	return updatedRows, nil
}

func (planner *IndexUpdatePlanner) executeDelete(data sql.DeleteCommand, x tx.Transaction) (int, error) {
	plan, err := newTablePlan(x, data.TableName, planner.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan := newSelectPlan(plan, data.Predicate)

	ii, err := planner.mdm.indexInfo(x, data.TableName)
	if err != nil {
		return 0, err
	}

	s, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}

	updateScan := s.(UpdateScan)
	defer updateScan.Close()

	delFromIdx := func() error {
		rid := updateScan.GetRID()
		for field, info := range ii {
			val, err := updateScan.Val(field)
			if err != nil {
				return err
			}

			idx := info.Open()
			defer idx.Close()
			if err := idx.Delete(val, rid); err != nil {
				return err
			}
		}
		return nil
	}

	c := 0
	// first, delete the record rid from every index
	for {
		err := updateScan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return c, err
		}

		if err := delFromIdx(); err != nil {
			return c, err
		}

		if err := updateScan.Delete(); err != nil {
			return c, err
		}

		c++
	}

	return c, nil
}

func (planner *IndexUpdatePlanner) executeCreateIndex(data sql.CreateIndexCommand, x tx.Transaction) (int, error) {
	if err := planner.mdm.createIndex(x, data.IndexName, data.TableName, data.TargetField); err != nil {
		return 0, err
	}

	return 0, nil
}

func (planner *IndexUpdatePlanner) executeCreateTable(data sql.CreateTableCommand, x tx.Transaction) (int, error) {
	schema := newSchema()
	for _, f := range data.Fields {
		schema.addField(f.Name, f.Type)
	}
	return 0, planner.mdm.createTable(data.TableName, schema, x)
}

func (planner *IndexUpdatePlanner) executeCreateView(data sql.CreateViewCommand, x tx.Transaction) (int, error) {
	return 0, planner.mdm.createView(data.ViewName, data.Definition(), x)
}
