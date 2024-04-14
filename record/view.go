package record

import (
	"io"

	"github.com/luigitni/simpledb/tx"
)

const maxViewDefinition = 100

type ViewManager struct {
	*TableManager
}

func NewViewManager(tm *TableManager) *ViewManager {
	return &ViewManager{
		tm,
	}
}

func (vm ViewManager) Init(trans tx.Transaction) {
	schema := NewSchema()
	schema.AddStringField("viewname", NameMaxLen)
	schema.AddStringField("viewdef", maxViewDefinition)
	vm.CreateTable("viewcat", schema, trans)
}

func (vm ViewManager) CreateView(vname string, vdef string, trans tx.Transaction) error {
	layout, err := vm.Layout("viewcat", trans)
	if err != nil {
		return err
	}

	ts := NewTableScan(trans, "viewcat", layout)
	if err := ts.SetString("viewname", vname); err != nil {
		return err
	}

	if err := ts.SetString("viewdef", vdef); err != nil {
		return err
	}

	ts.Close()
	return nil
}

func (vm ViewManager) ViewDefinition(vname string, trans tx.Transaction) (string, error) {
	layout, err := vm.Layout("viewcat", trans)
	if err != nil {
		return "", err
	}

	ts := NewTableScan(trans, "viewcat", layout)
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
