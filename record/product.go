package record

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

// Next implements Scan.
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
