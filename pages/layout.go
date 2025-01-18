package pages

import "github.com/luigitni/simpledb/types"

type Layout interface {
	FieldIndex(fname string) int
	FieldsCount() int
	FieldSize(fname string) types.Size
}
