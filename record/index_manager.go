package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

const (
	idxCatalogTableName = "idxcat"
	fieldIdxName        = "indexname"
)

type Index interface {
	BeforeFirst(searchKey file.Value) error
	Next() error
	DataRID() (RID, error)
	Insert(v file.Value, rid RID) error
	Delete(v file.Value, rid RID) error
	Close()
}

// indexInfo contains statistical information of an index.
// It also provides an Open method that opens a scannable index
// over the indexed field.
type indexInfo struct {
	idxName     string
	fieldName   string
	x           tx.Transaction
	tableSchema Schema
	idxLayout   Layout
	stats       statInfo
}

func newIndexInfo(x tx.Transaction, idxName string, fieldName string, tableSchema Schema, stats statInfo) *indexInfo {
	return &indexInfo{
		idxName:     idxName,
		fieldName:   fieldName,
		x:           x,
		tableSchema: tableSchema,
		idxLayout:   idxLayout(tableSchema, fieldName),
		stats:       stats,
	}
}

func idxLayout(tableSchema Schema, fieldName string) Layout {
	schema := newSchema()
	schema.addIntField("block")
	schema.addIntField("id")
	switch tableSchema.ftype(fieldName) {
	case file.INTEGER:
		schema.addIntField("dataval")
	case file.STRING:
		schema.addFixedLenStringField("dataval", tableSchema.flen(fieldName))
	}

	return NewLayout(schema)
}

// Open returns the index defined over the specified field indexInfo belongs to.
func (ii *indexInfo) Open() Index {
	ii.tableSchema = newSchema()
	idx, err := NewBTreeIndex(ii.x, ii.idxName, ii.idxLayout)
	if err != nil {
		panic(err)
	}

	return idx
}

func (ii *indexInfo) RecordsOutput() int {
	return ii.stats.records / ii.stats.distinctValues(ii.fieldName)
}

func (ii *indexInfo) BlocksAccessed() int {
	rpb := ii.x.BlockSize() / ii.idxLayout.slotsize
	numBlocks := ii.stats.records / rpb
	return BTreeIndexSearchCost(numBlocks, rpb)
}

func (ii *indexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}

	return ii.stats.distinctValues(fieldName)
}

// indexManager manages the catalog of indexes and keeps track
// of the tables and fields that each index is indexing.
// The indexManager looks into the index catalog to determine if a given field
// has a defined index and returns it to the planner.
type indexManager struct {
	l  Layout
	tm *tableManager
	sm *statManager
}

func indexCatalogSchema() Schema {
	schema := newSchema()
	schema.addFixedLenStringField(fieldIdxName, NameMaxLen)
	schema.addFixedLenStringField(catFieldTableName, NameMaxLen)
	schema.addFixedLenStringField(catFieldFieldName, NameMaxLen)
	return schema
}

func newIndexManager(tm *tableManager, sm *statManager) *indexManager {
	return &indexManager{
		l:  NewLayout(indexCatalogSchema()),
		tm: tm,
		sm: sm,
	}
}

func (im indexManager) init(x tx.Transaction) error {
	return im.tm.createTable(idxCatalogTableName, im.l.schema, x)
}

// createIndex stores the index metadata into the catalog.
func (im *indexManager) createIndex(x tx.Transaction, idxName string, tblName string, fldName string) error {
	ts := newTableScan(x, idxCatalogTableName, im.l)

	// todo: create fixed size string type
	size := file.StrLength(len(idxName)) + file.StrLength(len(tblName)) + file.StrLength(len(fldName))

	ts.Insert(size)
	defer ts.Close()

	if err := ts.SetString(fieldIdxName, idxName); err != nil {
		return err
	}

	if err := ts.SetString(catFieldTableName, tblName); err != nil {
		return err
	}

	if err := ts.SetString(catFieldFieldName, fldName); err != nil {
		return err
	}

	return nil
}

// indexInfo returns a map of indexInfo defined over the fields of the provided table.
func (im *indexManager) indexInfo(x tx.Transaction, tblName string) (map[string]*indexInfo, error) {

	m := map[string]*indexInfo{}

	scan := newTableScan(x, idxCatalogTableName, im.l)
	defer scan.Close()

	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		table, err := scan.String(catFieldTableName)
		if err != nil {
			return nil, err
		}

		if table != tblName {
			continue
		}

		idxName, err := scan.String(fieldIdxName)
		if err != nil {
			return nil, err
		}

		fldName, err := scan.String(catFieldFieldName)
		if err != nil {
			return nil, err
		}

		layout, err := im.tm.layout(tblName, x)
		if err != nil {
			return nil, err
		}

		stat, err := im.sm.statInfo(tblName, layout, x)
		if err != nil {
			return nil, err
		}

		ii := newIndexInfo(x, idxName, fldName, *layout.Schema(), stat)
		m[fldName] = ii
	}

	return m, nil
}
