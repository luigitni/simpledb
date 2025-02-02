package engine

import (
	"io"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

var (
	_ Plan = &sortPlan{}
	_ Scan = &sortScan{}
)

type sortPlan struct {
	p      Plan
	x      tx.Transaction
	schema Schema
	recordComparator
}

func newSortPlan(x tx.Transaction, plan Plan, sortFields []string) *sortPlan {
	return &sortPlan{
		p:      plan,
		x:      x,
		schema: plan.Schema(),
		recordComparator: recordComparator{
			sortFields: sortFields,
		},
	}
}

func (sp *sortPlan) Schema() Schema {
	return sp.schema
}

func (sp *sortPlan) Open() (Scan, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}

	defer src.Close()

	// a run is a sorted portion of a block, for example
	// 2 6 20 4 1 16 19 3 18 is made of the following runs:
	// 2 6 20
	// 4
	// 1 16 19
	// 3 18
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}

	for len(runs) > 2 {
		runs, err = sp.mergeOnce(runs)
		if err != nil {
			return nil, err
		}
	}

	return newSortScan(sp.recordComparator, runs)
}

// splitIntoRuns reads records from the source Scan into temporary tables
// such that each table contains a run.
// Each time a new run begins, the destination scan (the temp table) is closed
// and another one is created and opened.
func (sp *sortPlan) splitIntoRuns(src Scan) ([]*tmpTable, error) {
	var tables []*tmpTable

	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}

	if err := src.Next(); err != nil {
		return nil, err
	}

	currentTmpTable := newTmpTable(sp.x, sp.schema)
	tables = append(tables, currentTmpTable)

	currentScan := currentTmpTable.Open()

	for {
		err := sp.copy(src, currentScan)
		if err == io.EOF {
			break
		}

		if err != nil {
			currentScan.Close()
			return nil, err
		}

		less, err := sp.Less(src, currentScan)
		if err != nil {
			currentScan.Close()
			return nil, err
		}

		if less {
			currentScan.Close()

			currentTmpTable = newTmpTable(sp.x, sp.schema)
			tables = append(tables, currentTmpTable)

			currentScan = currentTmpTable.Open()
		}
	}

	currentScan.Close()
	return tables, nil
}

func (sp *sortPlan) mergeOnce(runs []*tmpTable) ([]*tmpTable, error) {
	var out []*tmpTable

	for len(runs) > 1 {
		first := runs[0]
		runs = runs[1:]

		second := runs[0]
		runs = runs[1:]

		n, err := sp.merge(first, second)
		if err != nil {
			return nil, err
		}

		out = append(out, n)
	}

	if len(runs) == 1 {
		out = append(out, runs[0])
	}

	return out, nil
}

func (sp *sortPlan) merge(first *tmpTable, second *tmpTable) (*tmpTable, error) {
	left := first.Open()
	defer left.Close()

	right := second.Open()
	defer right.Close()

	out := newTmpTable(sp.x, sp.schema)
	dst := out.Open()
	defer dst.Close()

	leftHasMore := left.Next()
	rightHasMore := right.Next()

	for {
		if leftHasMore != nil && leftHasMore != io.EOF {
			return nil, leftHasMore
		}

		if rightHasMore != nil && rightHasMore != io.EOF {
			return nil, rightHasMore
		}

		if leftHasMore == io.EOF && rightHasMore == io.EOF {
			break
		}

		if leftHasMore == nil && rightHasMore == nil {
			less, err := sp.Less(left, right)
			if err != nil {
				return nil, err
			}

			if less {
				leftHasMore = sp.copy(left, dst)
			} else {
				rightHasMore = sp.copy(right, dst)
			}
		} else if rightHasMore == io.EOF {
			for {
				leftHasMore = sp.copy(left, dst)
				if leftHasMore == io.EOF {
					break
				}

				if leftHasMore != nil {
					return nil, leftHasMore
				}
			}
		} else if leftHasMore == io.EOF {
			for {
				rightHasMore = sp.copy(right, dst)
				if rightHasMore == io.EOF {
					break
				}

				if rightHasMore != nil {
					return nil, rightHasMore
				}
			}
		}
	}

	return out, nil
}

func (sp *sortPlan) copy(src Scan, dst UpdateScan) error {
	var size storage.Offset
	vals := make(map[string]storage.Value)

	for _, f := range sp.schema.fields {
		v, err := src.Val(f)
		if err != nil {
			return err
		}

		t := sp.schema.ftype(f)

		size += v.Size(t)
		vals[f] = v
	}

	if err := dst.Insert(size); err != nil {
		return err
	}

	for f, v := range vals {
		if err := dst.SetVal(f, v); err != nil {
			return err
		}
	}

	return src.Next()
}

func (sp *sortPlan) BlocksAccessed() int {
	mp := newMaterializePlan(sp.x, sp.p)
	return mp.BlocksAccessed()
}

func (sp *sortPlan) RecordsOutput() int {
	return sp.p.RecordsOutput()
}

func (sp *sortPlan) DistinctValues(fieldName string) int {
	return sp.p.DistinctValues(fieldName)
}

type recordComparator struct {
	sortFields []string
}

func (rc recordComparator) Less(first Scan, second Scan) (bool, error) {
	for _, field := range rc.sortFields {
		f, err := first.Val(field)
		if err != nil {
			return false, err
		}

		s, err := second.Val(field)
		if err != nil {
			return false, err
		}

		if f.Less(s) {
			return true, nil
		}
	}

	return false, nil
}

type sortScan struct {
	recordComparator
	firstScan      UpdateScan
	secondScan     UpdateScan
	currentScan    UpdateScan
	firstHasMore   bool
	secondHasMore  bool
	savedPositions [2]RID
}

func newSortScan(recordComparator recordComparator, runs []*tmpTable) (*sortScan, error) {
	ss := &sortScan{
		recordComparator: recordComparator,
	}

	firstScan := runs[0].Open()
	firstHasMore, err := hasNextOrError(firstScan)
	if err != nil {
		return nil, err
	}

	ss.firstHasMore = firstHasMore
	ss.firstScan = firstScan

	if len(runs) > 1 {
		secondScan := runs[1].Open()
		secondHasMore, err := hasNextOrError(secondScan)
		if err != nil {
			return nil, err
		}

		ss.secondHasMore = secondHasMore
		ss.secondScan = secondScan
	}

	return ss, nil
}

func (ss *sortScan) BeforeFirst() error {
	if err := ss.firstScan.BeforeFirst(); err != nil {
		return err
	}

	hasNext, err := hasNextOrError(ss.firstScan)
	if err != nil {
		return err
	}

	ss.firstHasMore = hasNext

	if ss.secondScan != nil {
		if err := ss.secondScan.BeforeFirst(); err != nil {
			return err
		}

		hasNext, err := hasNextOrError(ss.secondScan)
		if err != nil {
			return err
		}

		ss.secondHasMore = hasNext
	}

	return nil
}

func (ss *sortScan) Next() error {
	if ss.currentScan == ss.firstScan {
		hasMore, err := hasNextOrError(ss.firstScan)
		if err != nil {
			return err
		}

		ss.firstHasMore = hasMore
	} else if ss.secondScan != nil && ss.currentScan == ss.secondScan {
		hasMore, err := hasNextOrError(ss.secondScan)
		if err != nil {
			return err
		}

		ss.secondHasMore = hasMore
	}

	if !ss.firstHasMore && !ss.secondHasMore {
		return io.EOF
	} else if ss.firstHasMore && ss.secondHasMore {
		less, err := ss.Less(ss.firstScan, ss.secondScan)
		if err != nil {
			return err
		}

		ss.currentScan = ss.secondScan

		if less {
			ss.currentScan = ss.firstScan
		}
	} else if ss.firstHasMore {
		ss.currentScan = ss.firstScan
	} else if ss.secondHasMore {
		ss.currentScan = ss.secondScan
	}

	return nil
}

func (ss *sortScan) Close() {
	ss.firstScan.Close()
	if ss.secondScan != nil {
		ss.secondScan.Close()
	}
}

func (ss *sortScan) Val(fieldName string) (storage.Value, error) {
	return ss.currentScan.Val(fieldName)
}

func (ss *sortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *sortScan) savePosition() {
	ss.savedPositions[0] = ss.firstScan.GetRID()
	if ss.secondScan != nil {
		ss.savedPositions[1] = ss.secondScan.GetRID()
	}
}

func (ss *sortScan) restorePosition() {
	ss.firstScan.MoveToRID(ss.savedPositions[0])
	if ss.secondScan != nil {
		ss.secondScan.MoveToRID(ss.savedPositions[1])
	}
}
