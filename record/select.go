package record

import (
	"errors"
	"io"
)

// Select is a relational algebra operator.
// Select returns a table that has the same
// columns of its input tabl but with some rows removed.
// Select scans are updatable
type Select struct {
	scan Scan
	// Predicate is any boolean combination of terms
	// and corresponds to a WHERE clause in SQL
	predicate Predicate
}

// NewSelect scan creates a new Select operator.
// It takes a table (Scan) as input and a Predicate.
func NewSelectScan(scan Scan, pred Predicate) Select {
	return Select{
		scan:      scan,
		predicate: pred,
	}
}

// Scan methods

// BeforeFirst implements Scan.
func (sel Select) BeforeFirst() {
	sel.scan.BeforeFirst()
}

// Close implements Scan.
func (sel Select) Close() {
	sel.scan.Close()
}

// GetInt implements Scan.
func (sel Select) GetInt(fname string) (int, error) {
	return sel.scan.GetInt(fname)
}

// GetString implements Scan.
func (sel Select) GetString(fname string) (string, error) {
	return sel.scan.GetString(fname)
}

// GetVal implements Scan.
func (sel Select) GetVal(fname string) (Constant, error) {
	return sel.scan.GetVal(fname)
}

// HasField implements Scan.
func (sel Select) HasField(fname string) bool {
	return sel.scan.HasField(fname)
}

// Next estabilishes a new current record.
// It loops through the underlying scan looking
// for a record that satisfies the underlying predicate.
// If such record is found, then it becomes the current record,
// otherwise the method returs a io.EOF error
func (sel Select) Next() error {
	for {
		err := sel.scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if err, ok := sel.predicate.IsSatisfied(sel.scan); ok {
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
func (sel Select) GetRID() RID {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		panic(errors.New("cannot update over anon update scan"))
	}

	return u.GetRID()
}

// MoveToRID implements UpdateScan.
func (sel Select) MoveToRID(rid RID) {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		panic(errors.New("cannot update over anon update scan"))
	}

	u.MoveToRID(rid)
}

// Insert implements UpdateScan.
func (sel *Select) Insert() error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}
	return u.Insert()
}

// SetInt implements UpdateScan.
func (sel Select) SetInt(fname string, v int) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetInt(fname, v)
}

// SetString implements UpdateScan.
func (sel Select) SetString(fname string, v string) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetString(fname, v)
}

// SetVal implements UpdateScan.
func (sel Select) SetVal(fname string, v Constant) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetVal(fname, v)
}
