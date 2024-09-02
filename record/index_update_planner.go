package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
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

	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}

	us := scan.(UpdateScan)
	defer us.Close()

	size := 0
	for _, v := range data.Values {
		size += v.Size()
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

	rid := updateScan.GetRID()

	updateIndex := func(oldVal file.Value, newVal file.Value) error {
		info, ok := ii[data.Field]
		if !ok {
			return nil
		}

		idx := info.Open()
		if err := idx.Delete(oldVal, rid); err != nil {
			return err
		}

		if err := idx.Insert(newVal, rid); err != nil {
			return err
		}

		return nil
	}

	c := 0

	for {
		err := updateScan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return c, err
		}

		newVal, err := data.NewValue.Evaluate(updateScan)
		if err != nil {
			return c, err
		}

		oldVal, err := updateScan.Val(data.Field)
		if err != nil {
			return c, err
		}

		if err := updateScan.SetVal(data.Field, newVal); err != nil {
			return c, err
		}

		if err := updateIndex(oldVal, newVal); err != nil {
			return c, err
		}

		c++
	}

	return c, nil
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
