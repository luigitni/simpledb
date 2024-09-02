package pages

type Layout interface {
	FieldIndex(fname string) int
	FieldsCount() int
}
