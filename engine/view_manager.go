package engine

import (
	"io"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

const (
	viewCatalogTableName = "views"
	fieldViewName        = "name"
	fieldViewDef         = "def"
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
	schema.addField(fieldViewName, storage.NAME)
	schema.addField(fieldViewDef, storage.TEXT)
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

	vn := storage.NewNameFromGoString(vname)

	if err := ts.SetVal("viewname", storage.ValueFromName(vn)); err != nil {
		return err
	}

	if err := ts.SetVal("viewdef", storage.ValueFromGoString(vdef)); err != nil {
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

		if storage.VarlenToGoString(s) == vname {
			res, err := ts.Varlen("viewdef")
			if err != nil {
				return "", err
			}
			return storage.VarlenToGoString(res), nil
		}
	}

	return "", ErrViewNotFound
}
