package engine

import (
	"github.com/luigitni/simpledb/tx"
)

// Plan calculates the cost of a query tree
// Like Scans, plans denote a query tree.
// Unlike Scans, plans access the metadata of the tables in the query instead of their data
// and compute the cost of the query by composing the underlying plans.
// When a SQL query is submitted, the database planner may create
// several plans and pick the most efficient.
// Then, it invokes Open to create the desired Scan.
// There is a Plan for each relational algebra operator.
type Plan interface {
	// Creates the desired Scan after the Plan has been selected
	// by the query planner
	Open() (Scan, error)
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	// Schema returns the schema of the OUTPUT table
	// The query planner can use this schema to verify
	// type correctness and to optimise the plan
	Schema() Schema
}

// tablePlan obtains its cost estimates directly from the metadata manager.
type tablePlan struct {
	tx        tx.Transaction
	tableName string
	layout    Layout
	info      statInfo
}

func newTablePlan(tx tx.Transaction, table string, md *MetadataManager) (tablePlan, error) {
	layout, err := md.layout(table, tx)
	if err != nil {
		return tablePlan{}, err
	}

	statInfo, err := md.statInfo(table, layout, tx)
	if err != nil {
		return tablePlan{}, err
	}

	return tablePlan{
		tx:        tx,
		tableName: table,
		layout:    layout,
		info:      statInfo,
	}, nil
}

func (p tablePlan) Open() (Scan, error) {
	return newTableScan(p.tx, p.tableName, p.layout), nil
}

func (p tablePlan) BlocksAccessed() int {
	// todo: check this
	return int(p.info.blocks)
}

func (p tablePlan) RecordsOutput() int {
	return p.info.records
}

func (p tablePlan) DistinctValues(fname string) int {
	return p.info.distinctValues(fname)
}

func (p tablePlan) Schema() Schema {
	return *p.layout.Schema()
}

// SelectPlan plans the cost of a SelectScan.
// The estimates of the plan depend on the underlying predicate.
// To calculate the number of records accessed, it uses the ReductionFactor
// of the predicate, which is the extent to which the size of the input table is reduced by the predicate.
// It uses the EquatesWithConstant method of the predicate to tell if the predicate
// is equating a field with a constant.
// Both the factors above influence the cost of a plan.
type SelectPlan struct {
	plan      Plan
	predicate Predicate
}

func newSelectPlan(plan Plan, predicate Predicate) SelectPlan {
	return SelectPlan{
		plan:      plan,
		predicate: predicate,
	}
}

func (sp SelectPlan) Open() (Scan, error) {
	sub, err := sp.plan.Open()
	if err != nil {
		return nil, err
	}

	return newSelectScan(sub, sp.predicate), nil
}

func (p SelectPlan) BlocksAccessed() int {
	return p.plan.BlocksAccessed()
}

func (p SelectPlan) RecordsOutput() int {
	return p.plan.RecordsOutput() / p.predicate.ReductionFactor(p)
}

func (p SelectPlan) DistinctValues(fieldName string) int {
	if _, ok := p.predicate.EquatesWithConstant(fieldName); ok {
		return 1
	}

	f := p.plan.DistinctValues(fieldName)

	otherFieldName, ok := p.predicate.EquatesWithField(fieldName)
	if ok {
		if s := p.plan.DistinctValues(otherFieldName); s < f {
			return s
		}
	}
	return f
}

func (p SelectPlan) Schema() Schema {
	return p.plan.Schema()
}

type ProjectPlan struct {
	plan   Plan
	schema Schema
}

func newProjectPlan(p Plan, fields []string) ProjectPlan {
	schema := newSchema()
	for _, f := range fields {
		schema.add(f, p.Schema())
	}
	return ProjectPlan{
		plan:   p,
		schema: schema,
	}
}

func (p ProjectPlan) Open() (Scan, error) {
	s, err := p.plan.Open()
	if err != nil {
		return nil, err
	}
	return newProjectScan(s, p.schema.fields), nil
}

func (p ProjectPlan) BlocksAccessed() int {
	return p.plan.BlocksAccessed()
}

func (p ProjectPlan) RecordsOutput() int {
	return p.plan.RecordsOutput()
}

func (p ProjectPlan) DistinctValues(fiedName string) int {
	return p.plan.DistinctValues(fiedName)
}

func (p ProjectPlan) Schema() Schema {
	return p.schema
}

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema Schema
}

func newProductPlan(p1 Plan, p2 Plan) ProductPlan {
	schema := newSchema()
	schema.addAll(p1.Schema())
	schema.addAll(p2.Schema())
	return ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
}

func (p ProductPlan) Open() (Scan, error) {
	p1, err := p.p1.Open()
	if err != nil {
		return nil, err
	}

	p2, err := p.p2.Open()
	if err != nil {
		return nil, err
	}

	return newProduct(p1, p2), nil
}

func (p ProductPlan) BlocksAccessed() int {
	tmp := p.p1.RecordsOutput() * p.p2.BlocksAccessed()
	return p.p1.BlocksAccessed() * tmp
}

func (p ProductPlan) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.p2.RecordsOutput()
}

func (p ProductPlan) DistinctValues(fieldName string) int {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	}

	return p.p2.DistinctValues(fieldName)
}

func (p ProductPlan) Schema() Schema {
	return p.schema
}
