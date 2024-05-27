package record

import (
	"github.com/luigitni/simpledb/file"
)

// Scan represents the output of a relational algebra query.
// Scan's methods are a subset of TableScan's.
// The output of a query is a table and values are accessed in the same way.
// A Scan corresponds also to a node in a query tree.
type Scan interface {
	BeforeFirst()

	Next() error

	GetInt(fname string) (int, error)

	GetString(fname string) (string, error)

	GetVal(fname string) (file.Value, error)

	HasField(fname string) bool

	Close()
}

// UpdateScan is an updatable scan, where an updatable scan
// is a Scan if every output record in it has a corresponding record
// in an underlying database table.
// In SimpleDB, the only two classes that implement UpdateScan
// are TableScan and SelectScan
type UpdateScan interface {
	Scan

	SetInt(fname string, v int) error

	SetString(fname string, v string) error

	SetVal(fname string, v file.Value) error

	Insert() error

	Delete() error

	GetRID() RID

	MoveToRID(rid RID)
}
