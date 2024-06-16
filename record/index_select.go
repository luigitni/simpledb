package record

import "github.com/luigitni/simpledb/file"

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

	scan := s.(*TableScan)
	idx := plan.indexInfo.Open()
	return NewIndexSelectScan(scan, idx, plan.val), nil
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

var _ Scan = &IndexSelectScan{}

type IndexSelectScan struct {
	tableScan *TableScan
	idx       Index
	val       file.Value
}

func NewIndexSelectScan(ts *TableScan, idx Index, val file.Value) *IndexSelectScan {
	scan := &IndexSelectScan{
		tableScan: ts,
		idx:       idx,
		val:       val,
	}

	scan.BeforeFirst()
	return scan
}

func (scan *IndexSelectScan) BeforeFirst() {
	if err := scan.idx.BeforeFirst(scan.val); err != nil {
		panic(err)
	}
}

func (scan *IndexSelectScan) Next() error {
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

func (scan *IndexSelectScan) GetInt(fname string) (int, error) {
	return scan.tableScan.GetInt(fname)
}

func (scan *IndexSelectScan) GetString(fname string) (string, error) {
	return scan.tableScan.GetString(fname)
}

func (scan *IndexSelectScan) GetVal(fname string) (file.Value, error) {
	return scan.tableScan.GetVal(fname)
}

func (scan *IndexSelectScan) HasField(fname string) bool {
	return scan.tableScan.HasField(fname)
}

func (scan *IndexSelectScan) Close() {
	scan.idx.Close()
	scan.tableScan.Close()
}
