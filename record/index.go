package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

const (
	idxCatalogTableName = "idxcat"
	fieldIdxName        = "indexName"
	fieldTableName      = "tableName"
	fieldFieldName      = "fieldName"
)

type Index interface {
	BeforeFirst(searchKey file.Value) error
	Next() error
	DataRID() (RID, error)
	Insert(v file.Value, rid RID) error
	Delete(v file.Value, rid RID) error
	Close()
}

type IndexInfo struct {
	idxName     string
	fieldName   string
	x           tx.Transaction
	tableSchema Schema
	idxLayout   Layout
	stats       StatInfo
}

func NewIndexInfo(x tx.Transaction, idxName string, fieldName string, tableSchema Schema, stats StatInfo) *IndexInfo {
	return &IndexInfo{
		idxName:     idxName,
		fieldName:   fieldName,
		x:           x,
		tableSchema: tableSchema,
		idxLayout:   idxLayout(tableSchema, fieldName),
		stats:       stats,
	}
}

func idxLayout(tableSchema Schema, fieldName string) Layout {
	schema := NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	switch tableSchema.Type(fieldName) {
	case file.INTEGER:
		schema.AddIntField("dataval")
	case file.STRING:
		schema.AddStringField("dataval", tableSchema.Length(fieldName))
	}

	return NewLayout(schema)
}

// Open returns the index.
// Not yet implemented
func (ii *IndexInfo) Open() Index {
	ii.tableSchema = NewSchema()
	idx, err := NewBTreeIndex(ii.x, ii.idxName, ii.idxLayout)
	if err != nil {
		panic(err)
	}

	return idx
}

func (ii *IndexInfo) RecordsOutput() int {
	return ii.stats.Records / ii.stats.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) BlocksAccessed() int {
	rpb := ii.x.BlockSize() / ii.idxLayout.slotsize
	numBlocks := ii.stats.Records / rpb
	return BTreeIndexSearchCost(numBlocks, rpb)
}

func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}

	return ii.stats.DistinctValues(fieldName)
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

func (im *IndexManager) IndexInfo(x tx.Transaction, tblName string) (map[string]*IndexInfo, error) {

	m := map[string]*IndexInfo{}

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
