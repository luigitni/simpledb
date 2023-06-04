package record

type Predicate struct{}

func (p Predicate) IsSatisfied(s Scan) bool {
	return true
}
