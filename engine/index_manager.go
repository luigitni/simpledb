package engine

import (
	"io"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

const (
	idxCatalogTableName = "idxcat"
	fieldIdxName        = "indexname"
)

type Index interface {
	BeforeFirst(searchKey storage.Value) error
	Next() error
	DataRID() (RID, error)
	Insert(v storage.Value, rid RID) error
	Delete(v storage.Value, rid RID) error
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
	schema.addField("block", storage.LONG)
	schema.addField("id", storage.INT)

	schema.addField("dataval", tableSchema.ftype(fieldName))

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
	rpb := int(ii.x.BlockSize() / ii.idxLayout.slotsize)
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
	schema.addField(fieldIdxName, storage.NAME)
	schema.addField(tableCatalogNameField, storage.NAME)
	schema.addField(fieldsCatalogNameField, storage.NAME)
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
	size := storage.UnsafeSizeOfStringAsVarlen(idxName) +
		storage.UnsafeSizeOfStringAsVarlen(tblName) +
		storage.UnsafeSizeOfStringAsVarlen(fldName)

	ts.Insert(storage.Offset(size))
	defer ts.Close()

	if err := ts.SetVal(fieldIdxName, storage.ValueFromGoString(idxName)); err != nil {
		return err
	}

	if err := ts.SetVal(tableCatalogNameField, storage.ValueFromGoString(tblName)); err != nil {
		return err
	}

	if err := ts.SetVal(fieldsCatalogNameField, storage.ValueFromGoString(fldName)); err != nil {
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

		table, err := scan.Val(tableCatalogNameField)
		if err != nil {
			return nil, err
		}

		if table.AsGoString() != tblName {
			continue
		}

		idxName, err := scan.Val(fieldIdxName)
		if err != nil {
			return nil, err
		}

		fldName, err := scan.Val(fieldsCatalogNameField)
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

		fn := fldName.AsGoString()

		ii := newIndexInfo(x, idxName.AsGoString(), fn, *layout.Schema(), stat)
		m[fn] = ii
	}

	return m, nil
}
