package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
)

var _ Plan = &IndexJoinPlan{}

type IndexJoinPlan struct {
	firstPlan  Plan
	secondPlan Plan
	ii         indexInfo
	joinField  string
	schema     Schema
}

func newIndexJoinPlan(firstPlan, secondPlan Plan, ii indexInfo, joinField string) *IndexJoinPlan {
	schema := newSchema()
	schema.addAll(firstPlan.Schema())
	schema.addAll(secondPlan.Schema())

	return &IndexJoinPlan{
		firstPlan:  firstPlan,
		secondPlan: secondPlan,
		ii:         ii,
		joinField:  joinField,
		schema:     schema,
	}
}

func (plan *IndexJoinPlan) Schema() Schema {
	return plan.schema
}

func (plan *IndexJoinPlan) Open() (Scan, error) {
	s, err := plan.firstPlan.Open()
	if err != nil {
		return nil, err
	}

	ss, err := plan.secondPlan.Open()
	if err != nil {
		return nil, err
	}

	ts := ss.(*TableScan)

	idx := plan.ii.Open()
	return newIndexJoinScan(s, idx, plan.joinField, ts), nil
}

func (plan *IndexJoinPlan) BlocksAccessed() int {
	return plan.firstPlan.BlocksAccessed() +
		(plan.firstPlan.RecordsOutput() + plan.ii.BlocksAccessed()) +
		plan.RecordsOutput()
}

func (plan *IndexJoinPlan) DistinctValues(fieldName string) int {
	if plan.firstPlan.Schema().hasField(fieldName) {
		return plan.firstPlan.DistinctValues(fieldName)
	}

	return plan.secondPlan.DistinctValues(fieldName)
}

func (plan *IndexJoinPlan) RecordsOutput() int {
	return plan.firstPlan.RecordsOutput() * plan.ii.RecordsOutput()
}

var _ Scan = &IndexJoinScan{}

type IndexJoinScan struct {
	lhs       Scan
	idx       Index
	joinField string
	rhs       *TableScan
}

func newIndexJoinScan(lhs Scan, idx Index, joinField string, rhs *TableScan) *IndexJoinScan {
	ijs := &IndexJoinScan{
		lhs:       lhs,
		idx:       idx,
		joinField: joinField,
		rhs:       rhs,
	}

	ijs.BeforeFirst()
	return ijs
}

func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.idx.Close()
	ijs.rhs.Close()
}

func (ijs *IndexJoinScan) BeforeFirst() {
	ijs.lhs.BeforeFirst()
	ijs.lhs.Next()
	ijs.resetIndex()
}

func (ijs *IndexJoinScan) Next() error {
	for {
		err := ijs.idx.Next()

		if err == nil {

			rid, err := ijs.idx.DataRID()
			if err != nil {
				return err
			}

			ijs.rhs.MoveToRID(rid)
			return nil
		}

		if err != io.EOF {
			return err
		}

		if err := ijs.lhs.Next(); err != nil {
			return err
		}

		ijs.resetIndex()
	}
}

func (ijs *IndexJoinScan) resetIndex() error {
	key, err := ijs.lhs.GetVal(ijs.joinField)
	if err != nil {
		return err
	}

	return ijs.idx.BeforeFirst(key)
}

func (ijs *IndexJoinScan) HasField(field string) bool {
	return ijs.rhs.HasField(field) || ijs.lhs.HasField(field)
}

func (ijs *IndexJoinScan) GetVal(field string) (file.Value, error) {
	if ijs.rhs.HasField(field) {
		return ijs.rhs.GetVal(field)
	}

	return ijs.lhs.GetVal(field)
}

func (ijs *IndexJoinScan) GetInt(field string) (int, error) {
	if ijs.rhs.HasField(field) {
		return ijs.rhs.GetInt(field)
	}

	return ijs.lhs.GetInt(field)
}

func (ijs *IndexJoinScan) GetString(field string) (string, error) {
	if ijs.rhs.HasField(field) {
		return ijs.rhs.GetString(field)
	}

	return ijs.lhs.GetString(field)
}
