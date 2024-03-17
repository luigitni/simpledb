package planner

import (
	"github.com/luigitni/simpledb/meta"
	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/tx"
)

// Plan calculates the cost of a query tree
// Like Scans, plans denote a query tree.
// Unlike Scans, plans access the metadata of the tables
// in the query instead of their data.
// When a SQL query is submitted, the database planner may create
// several plans and pick the most efficient.
// Then, it invokes Open to create the desired Scan
type Plan interface {
	// Creates the desired Scan after the Plan has been selected
	// by the query planner
	Open() record.Scan
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	// Schema returns the schema of the OUTPUT table
	// The query planner can use this schema to verify
	// type correctness and to optimise the plan
	Schema() record.Schema
}

type TablePlan struct {
	tx        tx.Transaction
	tableName string
	layout    record.Layout
	info      meta.StatInfo
}

func NewTablePlan(tx tx.Transaction, table string, md *meta.Manager) (TablePlan, error) {
	layout, err := md.Layout(table, tx)
	if err != nil {
		return TablePlan{}, err
	}

	statInfo, err := md.StatInfo(table, layout, tx)
	if err != nil {
		return TablePlan{}, err
	}

	return TablePlan{
		tx:        tx,
		tableName: table,
		layout:    layout,
		info:      statInfo,
	}, nil
}

func (p TablePlan) Open() record.Scan {
	return record.NewTableScan(p.tx, p.tableName, p.layout)
}

func (p TablePlan) BlocksAccessed() int {
	return p.info.Blocks
}

func (p TablePlan) RecordsOutput() int {
	return p.info.Records
}

func (p TablePlan) DistinctValues(fname string) int {
	return p.info.DistinctValues(fname)
}

func (p TablePlan) Schema() record.Schema {
	return *p.layout.Schema()
}

type SelectPlan struct {
	plan      Plan
	predicate record.Predicate
}

func NewSelectPlan(plan Plan, predicate record.Predicate) SelectPlan {
	return SelectPlan{
		plan:      plan,
		predicate: predicate,
	}
}

func (sp SelectPlan) Open() record.Scan {
	sub := sp.plan.Open()
	return record.NewSelectScan(sub, sp.predicate)
}

func (p SelectPlan) BlocksAccessed() int {
	return p.plan.BlocksAccessed()
}

func (p SelectPlan) RecordsOutput() int {
	return p.plan.RecordsOutput()
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

func (p SelectPlan) Schema() record.Schema {
	return p.plan.Schema()
}

type ProjectPlan struct {
	plan   Plan
	schema record.Schema
}

func NewProjectPlan(p Plan, fields record.FieldList) ProjectPlan {
	schema := record.NewSchema()
	for _, f := range fields {
		schema.Add(f, p.Schema())
	}
	return ProjectPlan{
		plan:   p,
		schema: schema,
	}
}

func (p ProjectPlan) Open() record.Scan {
	s := p.plan.Open()
	return record.NewProjectScan(s, p.schema.Fields())
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

func (p ProjectPlan) Schema() record.Schema {
	return p.schema
}

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema record.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) ProductPlan {
	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())
	return ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
}

func (p ProductPlan) Open() record.Scan {
	p1 := p.p1.Open()
	p2 := p.p2.Open()
	return record.NewProduct(p1, p2)
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

func (p ProductPlan) Schema() record.Schema {
	return p.schema
}
