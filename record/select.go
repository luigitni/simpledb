package record

import (
	"errors"
	"io"
)

type Select struct {
	scan      Scan
	predicate Predicate
}

func NewSelect(scan Scan, pred Predicate) Select {
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
func (sel Select) GetVal(fname string) (interface{}, error) {
	return sel.scan.GetVal(fname)
}

// HasField implements Scan.
func (sel Select) HasField(fname string) bool {
	return sel.scan.HasField(fname)
}

// Next implements Scan.
func (sel Select) Next() error {
	for {
		err := sel.scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if sel.predicate.IsSatisfied(sel.scan) {
			return nil
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
func (sel Select) GetRid() RID {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		panic(errors.New("cannot update over anon update scan"))
	}

	return u.GetRid()
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
func (sel Select) SetVal(fname string, v interface{}) error {
	u, ok := sel.scan.(UpdateScan)
	if !ok {
		return errors.New("cannot update over anon update scan")
	}

	return u.SetVal(fname, v)
}
