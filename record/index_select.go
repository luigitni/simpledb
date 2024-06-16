package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
)

var _ Plan = &IndexSelectPlan{}

type IndexSelectPlan struct {
	p         Plan
	indexInfo indexInfo
	val       file.Value
}

func NewIndexSelectPlan(p Plan, ii indexInfo, val file.Value) *IndexSelectPlan {
	return &IndexSelectPlan{
		p:         p,
		indexInfo: ii,
		val:       val,
	}
}

func (plan *IndexSelectPlan) Open() (Scan, error) {
	s, err := plan.p.Open()
	if err != nil {
		return nil, err
	}

	scan := s.(*tableScan)
	idx := plan.indexInfo.Open()
	return newIndexSelectScan(scan, idx, plan.val)
}

func (plan *IndexSelectPlan) BlocksAccessed() int {
	return plan.indexInfo.BlocksAccessed() + plan.RecordsOutput()
}

func (plan *IndexSelectPlan) RecordsOutput() int {
	return plan.indexInfo.RecordsOutput()
}

func (plan *IndexSelectPlan) DistinctValues(fieldName string) int {
	return plan.indexInfo.DistinctValues(fieldName)
}

func (plan *IndexSelectPlan) Schema() Schema {
	return plan.p.Schema()
}

var _ Scan = &indexSelectScan{}

type indexSelectScan struct {
	tableScan *tableScan
	idx       Index
	val       file.Value
}

func newIndexSelectScan(ts *tableScan, idx Index, val file.Value) (*indexSelectScan, error) {
	scan := &indexSelectScan{
		tableScan: ts,
		idx:       idx,
		val:       val,
	}

	if err := scan.BeforeFirst(); err != nil && err != io.EOF {
		return nil, err
	}

	return scan, nil
}

func (scan *indexSelectScan) BeforeFirst() error {
	return scan.idx.BeforeFirst(scan.val)
}

func (scan *indexSelectScan) Next() error {
	err := scan.idx.Next()
	if err == nil {
		rid, err := scan.idx.DataRID()
		if err != nil {
			return err
		}

		scan.tableScan.MoveToRID(rid)
	}

	return err
}

func (scan *indexSelectScan) GetInt(fname string) (int, error) {
	return scan.tableScan.GetInt(fname)
}

func (scan *indexSelectScan) GetString(fname string) (string, error) {
	return scan.tableScan.GetString(fname)
}

func (scan *indexSelectScan) GetVal(fname string) (file.Value, error) {
	return scan.tableScan.GetVal(fname)
}

func (scan *indexSelectScan) HasField(fname string) bool {
	return scan.tableScan.HasField(fname)
}

func (scan *indexSelectScan) Close() {
	scan.idx.Close()
	scan.tableScan.Close()
}
