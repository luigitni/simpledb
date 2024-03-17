package planner

import (
	"github.com/luigitni/simpledb/meta"
	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/tx"
)

type UpdatePlanner interface {
	// ExecuteInsert inserts
	ExecuteInsert(data record.InsertData, x tx.Transaction) (int, error)
	ExecuteDelete(data record.DeleteData, x tx.Transaction) (int, error)
	ExecuteModify(data record.ModifyData, x tx.Transaction) (int, error)
	ExecuteCreateTable(data record.CreateTableData, x tx.Transaction) (int, error)
	ExecuteCreateView(data record.CreateViewData, x tx.Transaction) (int, error)
	ExecuteCreateIndex(data record.CreateIndexData, x tx.Transaction) (int, error)
}

type BasicUpdatePlanner struct {
	mdm *meta.Manager
}

func NewBasicUpdatePlanner(mdm *meta.Manager) BasicUpdatePlanner {
	return BasicUpdatePlanner{
		mdm: mdm,
	}
}

func (bup BasicUpdatePlanner) ExecuteInsert(data record.InsertData, x tx.Transaction) (int, error) {
	p, err := NewTablePlan(x, data.TableName, bup.mdm)
	if err != nil {
		return 0, err
	}

	us := p.Open().(record.UpdateScan)
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

func (bup BasicUpdatePlanner) ExecuteCreateTable(data record.CreateTableData, x tx.Transaction) (int, error) {
	err := bup.mdm.CreateTable(data.TableName, data.Schema, x)
	return 0, err
}
