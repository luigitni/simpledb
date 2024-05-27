package record

import (
	"errors"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

type QueryPlanner interface {
	CreatePlan(data sql.Query, x tx.Transaction) (Plan, error)
}

type BasicQueryPlanner struct {
	mdm *MetadataManager
}

func NewBasicQueryPlanner(mdm *MetadataManager) BasicQueryPlanner {
	return BasicQueryPlanner{
		mdm: mdm,
	}
}

// CreatePlan creates a simple query plan using the following basic algorithm
//  1. Construct a plan for each table T in the <FROM> clause
//     a. If T is a stored table, the plan is a table plan for T
//     b. If T is a view, the plan is the result of calling this algorithm recursively on T's definition
//  2. Take the product of these table plans, in the order given
//  3. Select on the predicate in the <WHERE> clause
//  4. Project on the fields in the <SELECT> clause
func (bqp BasicQueryPlanner) CreatePlan(data sql.Query, x tx.Transaction) (Plan, error) {
	if len(data.Tables()) == 0 {
		return nil, errors.New("invalid query data: empty table set")
	}
	var plans []Plan
	for _, tName := range data.Tables() {
		viewDef, err := bqp.mdm.viewDefinition(tName, x)

		// the table is a view, recurse on T
		if err == nil {
			parser := sql.NewParser(viewDef)
			viewData, err := parser.Query()
			if err != nil {
				return nil, err
			}

			plan, err := bqp.CreatePlan(viewData, x)
			if err != nil {
				return nil, err
			}

			plans = append(plans, plan)
		}

		if errors.Is(err, ErrViewNotFound) {
			plan, err := NewTablePlan(x, tName, bqp.mdm)
			if err != nil {
				return nil, err
			}

			plans = append(plans, plan)
		}
	}

	// Create the product of all plans
	p := plans[0]
	for _, next := range plans[1:] {
		p = NewProductPlan(p, next)
	}

	p = newSelectPlan(p, data.Predicate())

	return NewProjectPlan(p, data.Fields()), nil
}
