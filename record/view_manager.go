package record

import (
	"io"

	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

const (
	viewCatalogTableName = "viewcat"
	fieldViewName        = "viewname"
	fieldViewDef         = "viewdef"
	maxViewDefinition    = 100
)

// viewManager stores view definitions in the view catalog.
// Each view is stored as a single record into the viewcat table.
type viewManager struct {
	*tableManager
}

func newViewManager(tm *tableManager) *viewManager {
	return &viewManager{
		tm,
	}
}

func (vm viewManager) init(trans tx.Transaction) error {
	schema := newSchema()
	schema.addFixedLenStringField(fieldViewName, NameMaxLen)
	schema.addFixedLenStringField(fieldViewDef, maxViewDefinition)
	return vm.createTable(viewCatalogTableName, schema, trans)
}

// createView adds a view entry into the view catalog.
func (vm viewManager) createView(vname string, vdef string, trans tx.Transaction) error {
	layout, err := vm.layout("viewcat", trans)
	if err != nil {
		return err
	}

	ts := newTableScan(trans, "viewcat", layout)
	defer ts.Close()

	if err := ts.SetVal("viewname", types.ValueFromGoString(vname)); err != nil {
		return err
	}

	if err := ts.SetVal("viewdef", types.ValueFromGoString(vdef)); err != nil {
		return err
	}

	return nil
}

// viewDefinition looks within the view catalog table for the requested view definition.
// If the view cannot be found returns an ErrViewNotFound
func (vm viewManager) viewDefinition(vname string, trans tx.Transaction) (string, error) {
	layout, err := vm.layout("viewcat", trans)
	if err != nil {
		return "", err
	}

	ts := newTableScan(trans, "viewcat", layout)
	defer ts.Close()

	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		s, err := ts.Varlen("viewname")
		if err != nil {
			return "", err
		}

		if types.UnsafeVarlenToGoString(s) == vname {
			res, err := ts.Varlen("viewdef")
			if err != nil {
				return "", err
			}
			return types.UnsafeVarlenToGoString(res), nil
		}
	}

	return "", ErrViewNotFound
}
