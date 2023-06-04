package record

import "errors"

type Project struct {
	scan   Scan
	fields map[string]struct{}
}

var ErrNoField = errors.New("field not found")

func NewProject(scan Scan, fields []string) Project {
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

// GetInt implements Scan.
func (project Project) GetInt(fname string) (int, error) {
	if !project.HasField(fname) {
		return 0, ErrNoField
	}
	return project.scan.GetInt(fname)
}

// GetString implements Scan.
func (project Project) GetString(fname string) (string, error) {
	if !project.HasField(fname) {
		return "", ErrNoField
	}
	return project.scan.GetString(fname)
}

// GetVal implements Scan.
func (project Project) GetVal(fname string) (interface{}, error) {
	if !project.HasField(fname) {
		return nil, ErrNoField
	}
	return project.scan.GetVal(fname)
}

// Next implements Scan.
func (project Project) Next() error {
	return project.scan.Next()
}
