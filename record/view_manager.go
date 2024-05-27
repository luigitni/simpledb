package record

import (
	"io"

	"github.com/luigitni/simpledb/tx"
)

const (
	viewCatalogTableName = "viewcat"
	fieldViewName        = "viewname"
	fieldViewDef         = "viewdef"
	maxViewDefinition    = 100
)

// ViewManager stores view definitions in the view catalog.
// Each view is stored as a single record into the viewcat table.
type ViewManager struct {
	*TableManager
}

func NewViewManager(tm *TableManager) *ViewManager {
	return &ViewManager{
		tm,
	}
}

func (vm ViewManager) Init(trans tx.Transaction) {
	schema := newSchema()
	schema.addStringField(fieldViewName, NameMaxLen)
	schema.addStringField(fieldViewDef, maxViewDefinition)
	vm.createTable(viewCatalogTableName, schema, trans)
}

// createView adds a view entry into the view catalog.
func (vm ViewManager) createView(vname string, vdef string, trans tx.Transaction) error {
	layout, err := vm.layout("viewcat", trans)
	if err != nil {
		return err
	}

	ts := newTableScan(trans, "viewcat", layout)
	if err := ts.SetString("viewname", vname); err != nil {
		return err
	}

	if err := ts.SetString("viewdef", vdef); err != nil {
		return err
	}

	ts.Close()
	return nil
}

// viewDefinition looks within the view catalog table for the requested view definition.
func (vm ViewManager) viewDefinition(vname string, trans tx.Transaction) (string, error) {
	layout, err := vm.layout("viewcat", trans)
	if err != nil {
		return "", err
	}

	ts := newTableScan(trans, "viewcat", layout)
	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		s, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}

		if s == vname {
			res, err := ts.GetString("viewdef")
			if err != nil {
				return "", err
			}
			return res, nil
		}
	}

	return "", io.EOF
}
