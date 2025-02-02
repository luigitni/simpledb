package engine

import (
	"io"

	"github.com/luigitni/simpledb/storage"
)

// Scan represents the output of a relational algebra query.
// Scan's methods are a subset of TableScan's.
// The output of a query is a table and values are accessed in the same way.
// A Scan corresponds also to a node in a query tree.
type Scan interface {
	BeforeFirst() error

	Next() error

	Val(fname string) (storage.Value, error)

	HasField(fname string) bool

	Close()
}

func hasNextOrError(scan Scan) (bool, error) {
	err := scan.Next()
	if err == nil {
		return true, nil
	}

	if err == io.EOF {
		return false, nil
	}

	return false, err
}

// UpdateScan is an updatable scan, where an updatable scan
// is a Scan if every output record in it has a corresponding record
// in an underlying database table.
// In SimpleDB, the only two classes that implement UpdateScan
// are TableScan and SelectScan
type UpdateScan interface {
	Scan

	SetVal(fname string, v storage.Value) error

	Insert(recordSize storage.Offset) error

	Update(recordSize storage.Offset) error

	Delete() error

	GetRID() RID

	MoveToRID(rid RID)
}
