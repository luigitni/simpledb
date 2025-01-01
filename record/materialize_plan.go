package record

import (
	"io"

	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

var _ Plan = &materializePlan{}

// materializePlan creates a new temporary table and copies the source
// plan into it.
type materializePlan struct {
	srcPlan Plan
	x       tx.Transaction
}

func newMaterializePlan(x tx.Transaction, srcPlan Plan) *materializePlan {
	return &materializePlan{
		srcPlan: srcPlan,
		x:       x,
	}
}

func (mp *materializePlan) Open() (Scan, error) {
	schema := mp.srcPlan.Schema()
	tmpTable := newTmpTable(mp.x, schema)

	src, err := mp.srcPlan.Open()
	if err != nil {
		return nil, err
	}

	defer src.Close()

	dst := tmpTable.Open()

	for {
		err := src.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		size := 0
		vals := make(map[string]types.Value)
		for _, fname := range schema.fields {
			v, err := src.Val(fname)
			if err != nil {
				return nil, err
			}
			vals[fname] = v
			size += v.Size()
		}

		if err := dst.Insert(size); err != nil {
			return nil, err
		}

		for fname, v := range vals {
			dst.SetVal(fname, v)
		}
	}

	if err := dst.BeforeFirst(); err != nil {
		return nil, err
	}

	return dst, nil
}

func (mp *materializePlan) Schema() Schema {
	return mp.srcPlan.Schema()
}

func (mp *materializePlan) BlocksAccessed() int {
	layout := NewLayout(mp.srcPlan.Schema())
	recordsPerBlock := float64(mp.x.BlockSize()) / float64(layout.slotsize)
	return int(float64(mp.srcPlan.RecordsOutput()) / recordsPerBlock)
}

func (mp *materializePlan) DistinctValues(fieldName string) int {
	return mp.srcPlan.DistinctValues(fieldName)
}

func (mp *materializePlan) RecordsOutput() int {
	return mp.srcPlan.RecordsOutput()
}
