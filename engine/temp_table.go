package engine

import (
	"fmt"
	"sync/atomic"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

var nextTableNum int64

type tmpTable struct {
	x       tx.Transaction
	tblName string
	layout  Layout
}

func newTmpTable(x tx.Transaction, schema Schema) *tmpTable {
	return &tmpTable{
		x:       x,
		tblName: nextTableName(),
		layout:  NewLayout(schema),
	}
}

func (tt *tmpTable) Open() UpdateScan {
	return newTableScan(tt.x, tt.tblName, tt.layout)
}

func nextTableName() string {
	v := atomic.AddInt64(&nextTableNum, 1)
	return fmt.Sprintf(file.TmpTablePrefix, v)
}
