package record

import "errors"

// Project is a relational algebra operator.
// Project returns a table that has the same rows
// of its input table, but with some columns removed.
type Project struct {
	scan   Scan
	// fields is the list of output fields.
	fields map[string]struct{}
}

var ErrNoField = errors.New("field not found")

func NewProjectScan(scan Scan, fields []string) Project {
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
func (project Project) BeforeFirst() {
	project.scan.BeforeFirst()
}

// Close implements Scan.
func (project Project) Close() {
	project.scan.Close()
}

// GetInt checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) GetInt(fname string) (int, error) {
	if !project.HasField(fname) {
		return 0, ErrNoField
	}
	return project.scan.GetInt(fname)
}

// GetString checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) GetString(fname string) (string, error) {
	if !project.HasField(fname) {
		return "", ErrNoField
	}
	return project.scan.GetString(fname)
}

// GetVal checks if the specified fieldname is in the list.
// If it is, it calls the underlying scan, if not, it returns an
// ErrNoField error
func (project Project) GetVal(fname string) (Constant, error) {
	if !project.HasField(fname) {
		return Constant{}, ErrNoField
	}
	return project.scan.GetVal(fname)
}

// Next implements Scan.
func (project Project) Next() error {
	return project.scan.Next()
}
