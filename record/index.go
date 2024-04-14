package record

import (
	"io"

	"github.com/luigitni/simpledb/tx"
)

const (
	idxCatalogTableName = "idxcat"
	fieldIdxName        = "indexName"
	fieldTableName      = "tableName"
	fieldFieldName      = "fieldName"
)

type IndexInfo struct {
	idxName   string
	fieldName string
	x         tx.Transaction
	schema    Schema
	layout    Layout
	stats     StatInfo
}

func NewIndexInfo(x tx.Transaction, idxName string, fieldName string, schema Schema, stats StatInfo) IndexInfo {
	return IndexInfo{
		idxName:   idxName,
		fieldName: fieldName,
		x:         x,
		schema:    schema,
		layout:    Layout{},
		stats:     stats,
	}
}

// Open returns the index.
// Not yet implemented
func (ii IndexInfo) Open() interface{} {
	return nil
}

type IndexManager struct {
	layout Layout
	tm     *TableManager
	sm     *StatManager
}

func NewIndexManager(isNew bool, tm *TableManager, sm *StatManager, x tx.Transaction) (*IndexManager, error) {
	if isNew {
		schema := NewSchema()
		schema.AddStringField(fieldIdxName, NameMaxLen)
		schema.AddStringField(fieldTableName, NameMaxLen)
		schema.AddStringField(fieldFieldName, NameMaxLen)
		tm.CreateTable(idxCatalogTableName, schema, x)
	}

	layout, err := tm.Layout(idxCatalogTableName, x)
	if err != nil {
		return nil, err
	}

	return &IndexManager{
		layout: layout,
		tm:     tm,
		sm:     sm,
	}, nil
}

func (im *IndexManager) CreateIndex(x tx.Transaction, idxName string, tblName string, fldName string) error {
	ts := NewTableScan(x, idxCatalogTableName, im.layout)

	ts.Insert()
	defer ts.Close()

	if err := ts.SetString(fieldIdxName, idxName); err != nil {
		return err
	}

	if err := ts.SetString(fieldTableName, tblName); err != nil {
		return err
	}

	if err := ts.SetString(fieldFieldName, fldName); err != nil {
		return err
	}

	return nil
}

func (im *IndexManager) IndexInfo(x tx.Transaction, tblName string) (map[string]IndexInfo, error) {

	m := map[string]IndexInfo{}

	scan := NewTableScan(x, tblName, im.layout)
	defer scan.Close()

	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		t, err := scan.GetString("tablename")
		if err != nil {
			return nil, err
		}

		if t != tblName {
			continue
		}

		idxName, err := scan.GetString(fieldIdxName)
		if err != nil {
			return nil, err
		}

		fldName, err := scan.GetString(fieldFieldName)
		if err != nil {
			return nil, err
		}

		layout, err := im.tm.Layout(tblName, x)
		if err != nil {
			return nil, err
		}

		stat, err := im.sm.StatInfo(tblName, layout, x)
		if err != nil {
			return nil, err
		}

		ii := NewIndexInfo(x, idxName, fldName, *layout.Schema(), stat)
		m[fldName] = ii
	}

	return m, nil
}
