package pages

import "github.com/luigitni/simpledb/storage"

type Layout interface {
	FieldIndex(fname string) int
	FieldsCount() int
	FieldSize(fname string) storage.Size
	FieldSizeByIndex(idx int) storage.Size
}
