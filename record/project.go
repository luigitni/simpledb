package record

import (
	"errors"

	"github.com/luigitni/simpledb/types"
)

// Project is a relational algebra operator.
// Project returns a table that has the same rows
// of its input table, but with some columns removed.
// A Project Scan has a single underlying scan
// and because it does not access any additional blocks compared with its underlying scan,
// its cost is exactly the same.
type Project struct {
	scan Scan
	// fields is the list of output fields.
	fields map[string]struct{}
}

var ErrNoField = errors.New("field not found")

func newProjectScan(scan Scan, fields []string) Project {
	m := make(map[string]struct{})
	for _, f := range fields {
		m[f] = struct{}{}
	}
	return Project{
		scan:   scan,
		fields: m,
	}
}

func (project Project) HasField(fname string) bool {
	_, ok := project.fields[fname]
	return ok
}

// BeforeFirst implements Scan.
func (project Project) BeforeFirst() error {
	return project.scan.BeforeFirst()
}

// Close implements Scan.
func (project Project) Close() {
	project.scan.Close()
}

// Int checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) Int(fname string) (int, error) {
	if !project.HasField(fname) {
		return 0, ErrNoField
	}
	return project.scan.Int(fname)
}

// String checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) String(fname string) (string, error) {
	if !project.HasField(fname) {
		return "", ErrNoField
	}
	return project.scan.String(fname)
}

// Val checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) Val(fname string) (types.Value, error) {
	if !project.HasField(fname) {
		return types.Value{}, ErrNoField
	}
	return project.scan.Val(fname)
}

// Next implements Scan.
func (project Project) Next() error {
	return project.scan.Next()
}
