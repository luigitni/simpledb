package record

import (
	"errors"
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/sql"
)

type Predicate interface {
	IsSatisfied(plan sql.Scan) (bool, error)
	EquatesWithConstant(fieldName string) (file.Value, bool)
	EquatesWithField(fieldname string) (string, bool)
	ReductionFactor(plan sql.Plan) int
	// Return the subpredicate consisting of terms that apply
	// to the joined schema, but not to either schema separately
	JoinSubPredicate(joined sql.Schema, first sql.Schema, second sql.Schema) (sql.Predicate, bool)
	// Return the sub-predicate that applies to schema
	SelectSubPredicate(schema sql.Schema) (sql.Predicate, bool)
}

// Select is a relational algebra operator.
// Select returns a table that has the same
// columns of its input tabl but with some rows removed.
// Select scans are updatable.
// A Select scan has a single underlying scan. When the Next() method is invoked,
// the underlying scan Next() is called until it returns false.
// Iterating through a Select scan accesses exactly the same blocks as the underlying scan.
// The number of records in the output of a Select Scan depend
// on the predicate and on the distribution of the matching records.
type Select struct {
	scan Scan
	// Predicate is any boolean combination of terms
	// and corresponds to a WHERE clause in SQL
	predicate Predicate
}

// NewSelect scan creates a new Select operator.
// It takes a table (Scan) as input and a Predicate.
func newSelectScan(scan Scan, pred Predicate) *Select {
	return &Select{
		scan:      scan,
		predicate: pred,
	}
}

// Scan methods

// BeforeFirst implements Scan.
func (sel *Select) BeforeFirst() error {
	return sel.scan.BeforeFirst()
}

// Close implements Scan.
func (sel *Select) Close() {
	sel.scan.Close()
}

// Int implements Scan.
func (sel *Select) Int(fname string) (int, error) {
	return sel.scan.Int(fname)
}

// String implements Scan.
func (sel *Select) String(fname string) (string, error) {
	return sel.scan.String(fname)
}

// Val implements Scan.
func (sel *Select) Val(fname string) (file.Value, error) {
	return sel.scan.Val(fname)
}

// HasField implements Scan.
func (sel *Select) HasField(fname string) bool {
	return sel.scan.HasField(fname)
}

// Next estabilishes a new current record.
// It loops through the underlying scan looking
// for a record that satisfies the underlying predicate.
// If such record is found, then it becomes the current record,
// otherwise the method returs a io.EOF error
func (sel *Select) Next() error {
	for {
		err := sel.scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if ok, err := sel.predicate.IsSatisfied(sel.scan); ok {
			return err
		}
	}

	return io.EOF
}

// UpdateScan methods

// Delete implements UpdateScan.
func (sel *Select) Delete() error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.Delete()
}

// GetRid implements UpdateScan.
func (sel *Select) GetRID() RID {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		panic(errors.New("cannot update over anon update scan"))
	}

	return u.GetRID()
}

// MoveToRID implements UpdateScan.
func (sel *Select) MoveToRID(rid RID) {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		panic(errors.New("cannot update over anon update scan"))
	}

	u.MoveToRID(rid)
}

// Insert implements UpdateScan.
func (sel *Select) Insert(size int) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot insert over anon update scan")
	}
	return u.Insert(size)
}

func (sel *Select) Update(size int) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}
	return u.Update(size)
}

// SetInt implements UpdateScan.
func (sel *Select) SetInt(fname string, v int) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetInt(fname, v)
}

// SetString implements UpdateScan.
func (sel *Select) SetString(fname string, v string) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetString(fname, v)
}

// SetVal implements UpdateScan.
func (sel *Select) SetVal(fname string, v file.Value) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetVal(fname, v)
}
