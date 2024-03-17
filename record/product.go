package record

// Product is a relational algebra operator.
// Product takes two tables as input and returns
// all possible combinations of their records.
type Product struct {
	first  Scan
	second Scan
}

func NewProduct(first Scan, second Scan) Scan {
	first.Next()
	return Product{
		first:  first,
		second: second,
	}
}

// BeforeFirst implements Scan.
func (pr Product) BeforeFirst() {
	pr.first.BeforeFirst()
	pr.first.Next()
	pr.second.BeforeFirst()
}

// Close implements Scan.
func (pr Product) Close() {
	pr.first.Close()
	pr.second.Close()
}

// GetInt implements Scan.
func (pr Product) GetInt(fname string) (int, error) {
	if pr.first.HasField(fname) {
		return pr.first.GetInt(fname)
	}

	return pr.second.GetInt(fname)
}

// GetString implements Scan.
func (pr Product) GetString(fname string) (string, error) {
	if pr.first.HasField(fname) {
		return pr.first.GetString(fname)
	}

	return pr.second.GetString(fname)
}

// GetVal implements Scan.
func (pr Product) GetVal(fname string) (Constant, error) {
	if pr.first.HasField(fname) {
		return pr.first.GetVal(fname)
	}

	return pr.second.GetVal(fname)
}

// HasField implements Scan.
func (pr Product) HasField(fname string) bool {
	return pr.first.HasField(fname) || pr.second.HasField(fname)
}

// Next iterates through all possible combinations of records of the ProductScan's input.
// Each call to Next moves the current record to the next record of the second input.
// If such record exists, Next returns.
// Otherwise, the iteration on the second input completes and the Scan moves to the next item
// of the first input.
func (pr Product) Next() error {
	if pr.second.Next() == nil {
		return nil
	}
	pr.second.BeforeFirst()
	if err := pr.second.Next(); err != nil {
		return err
	}

	if err := pr.first.Next(); err != nil {
		return err
	}

	return nil
}
