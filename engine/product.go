package engine

import (
	"io"

	"github.com/luigitni/simpledb/storage"
)

// Product is a relational algebra operator.
// Product takes two tables as input and returns
// all possible combinations of their records.
// A Product Scan has two underlying scans.
// Its ouput is made of all the combinations of records from Scan 1 and Scan 2.
// When the Product Scan is traversed, Scan 1 will be traversed once, and Scan 2
// will be traversed once for each record of Scan 1 that is matched (R).
// That is:
// Blocks accessed = B(s1) + (R(s1) * B(s2))
// Output = R(s1) + R(s2).
// The cost will be lowest when S1 has less records per block.
type Product struct {
	first  Scan
	second Scan
}

func newProduct(first Scan, second Scan) Scan {
	first.Next()
	return Product{
		first:  first,
		second: second,
	}
}

// BeforeFirst implements Scan.
func (pr Product) BeforeFirst() error {
	if err := pr.first.BeforeFirst(); err != nil {
		return err
	}

	if err := pr.first.Next(); err != nil && err != io.EOF {
		return err
	}

	return pr.second.BeforeFirst()
}

// Close implements Scan.
func (pr Product) Close() {
	pr.first.Close()
	pr.second.Close()
}

// Val implements Scan.
func (pr Product) Val(fname string) (storage.Value, error) {
	if pr.first.HasField(fname) {
		return pr.first.Val(fname)
	}

	return pr.second.Val(fname)
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
