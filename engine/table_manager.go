package engine

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

const (
	// table catalog
	tableCatalogTableName      = "tables"
	tableCatalogNameField      = "name"
	tableCatalogNumColumnField = "columns"

	sizeOfTableCatalogRecord = storage.SizeOfName + // table name
		storage.SizeOfSmallInt // number of columns

	fieldsCatalogTableName      = "fields"
	fieldsCatalogNameField      = "name"
	fieldsCatalogTableNameField = "table"
	fieldsCatalogTypeIDField    = "type_id"
	fieldsCatalogTypeNameField  = "type_name"
	fieldsCatalogSizeField      = "size"
	fieldsCatalogIndexField     = "index"

	sizeOfFieldsCatalogRecord = storage.SizeOfName + // field name
		storage.SizeOfName + // table name
		storage.SizeOfName + // field type
		storage.SizeOfSmallInt + // field size
		storage.SizeOfName // field index
)

var ErrViewNotFound = errors.New("cannot find table in catalog")

// tableManager handles table metadata, which describes the structure
// of each table's records.
// Table metadata is held in two tables:
//   - "tblcat" stores metadata specific to each table and has the following fields:
//     -- TblName: name of the table
//   - "fldcat" stores metadata of each field in each table.
//     Each record in the table represents a table field, and has the following keys:
//     -- TblName: name of the table the field belongs to.
//     -- FldName: name of the field.
//     -- Type: type of the field.
//     -- FldIdx: index of the field in the table's record layout.
type tableManager struct {
	// tablesCatalog is the catalog table for tables
	tablesCatalog Layout
	// fieldsCatalog is the catalog table for fields
	fieldsCatalog Layout
}

func newTableManager() *tableManager {
	tablesCatalog := newSchema()
	tablesCatalog.addField(tableCatalogNameField, storage.NAME)
	tablesCatalog.addField(tableCatalogNumColumnField, storage.SMALLINT)

	tablesCatalogLayout := NewLayout(tablesCatalog)

	fieldsCatalog := newSchema()
	// name of the field
	fieldsCatalog.addField(fieldsCatalogNameField, storage.NAME)
	// name of the table the field belongs to
	fieldsCatalog.addField(fieldsCatalogTableNameField, storage.NAME)
	// id of the type of the field
	fieldsCatalog.addField(fieldsCatalogTypeIDField, storage.SMALLINT)
	// name of the type of the field
	fieldsCatalog.addField(fieldsCatalogTypeNameField, storage.NAME)
	// size of the field in bytes
	fieldsCatalog.addField(fieldsCatalogSizeField, storage.SMALLINT)
	// index of the field within the record
	fieldsCatalog.addField(fieldsCatalogIndexField, storage.SMALLINT)

	fieldsCatalogLayout := NewLayout(fieldsCatalog)

	return &tableManager{
		tablesCatalog: tablesCatalogLayout,
		fieldsCatalog: fieldsCatalogLayout,
	}
}

func (tm *tableManager) init(x tx.Transaction) error {
	if err := tm.createTable(tableCatalogTableName, *tm.tablesCatalog.Schema(), x); err != nil {
		return err
	}

	if err := tm.createTable(fieldsCatalogTableName, *tm.fieldsCatalog.Schema(), x); err != nil {
		return err
	}

	return nil
}

func (tm *tableManager) tableExists(tblname string, tr tx.Transaction) bool {
	tcat := newTableScan(tr, tableCatalogTableName, tm.tablesCatalog)

	q := "SELECT tblname FROM tblcat WHERE tblname = " + tblname
	p := sql.NewParser(q)
	data, err := p.Query()
	if err != nil {
		panic(err)
	}

	sel := newSelectScan(tcat, data.Predicate())
	for {
		err := sel.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		v, err := sel.Val(tableCatalogNameField)
		if err != nil {
			panic(err)
		}

		if v.AsGoString() == tblname {
			return true
		}
	}

	return false
}

// createTable takes the table's name and schema and defines the record format
// by calculating each fields offset.
// It adds the newly created table as a record into the table catalog file and then
// adds each field of the table to the field catalog.
func (tm *tableManager) createTable(tblname string, sch Schema, x tx.Transaction) error {
	tcat := newTableScan(x, tableCatalogTableName, tm.tablesCatalog)
	defer tcat.Close()

	// add the new table into the table catalog
	// the table only has one field, the table name
	if err := tcat.Insert(storage.Offset(sizeOfTableCatalogRecord)); err != nil {
		return err
	}

	nameBuf := storage.NewNameFromGoString(tblname)

	if err := tcat.SetVal(tableCatalogNameField, storage.ValueFromName(nameBuf)); err != nil {
		return err
	}

	columns := storage.SmallInt(len(sch.fields))
	if err := tcat.SetVal(
		tableCatalogNumColumnField,
		storage.ValueFromInteger[storage.SmallInt](storage.SizeOfSmallInt, columns),
	); err != nil {
		return err
	}

	// for each schema field, insert a record into the field catalog.
	fcat := newTableScan(x, fieldsCatalogTableName, tm.fieldsCatalog)
	defer fcat.Close()

	for _, fname := range sch.fields {
		// scan up to the first available slot and add the field data to the field catalog
		if err := fcat.Insert(storage.Offset(sizeOfFieldsCatalogRecord)); err != nil {
			return err
		}

		nameBuf.WriteGoString(fname)
		if err := fcat.SetVal(fieldsCatalogNameField, storage.ValueFromName(nameBuf)); err != nil {
			return err
		}

		nameBuf.WriteGoString(tblname)
		if err := fcat.SetVal(fieldsCatalogTableNameField, storage.ValueFromName(nameBuf)); err != nil {
			return err
		}

		info := sch.finfo(fname)

		if err := fcat.SetVal(
			fieldsCatalogTypeIDField,
			storage.ValueFromInteger[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(info.Type)),
		); err != nil {
			return err
		}

		nameBuf.WriteGoString(sch.ftype(fname).String())
		if err := fcat.SetVal(fieldsCatalogTypeNameField, storage.ValueFromName(nameBuf)); err != nil {
			return err
		}

		if err := fcat.SetVal(
			fieldsCatalogSizeField,
			storage.ValueFromInteger[storage.Size](storage.SizeOfSize, info.Type.Size()),
		); err != nil {
			return err
		}

		if err := fcat.SetVal(
			fieldsCatalogIndexField,
			storage.ValueFromInteger[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(info.Index)),
		); err != nil {
			return err
		}
	}

	return nil
}

// layout opens two table scans, one into the table catalog table and the other one
// into the fields catalog, and retrieves the layout of the requested table.
func (tm *tableManager) layout(tblname string, x tx.Transaction) (Layout, error) {
	var empty Layout

	// check the table exists
	tcat := newTableScan(x, tableCatalogTableName, tm.tablesCatalog)
	defer tcat.Close()
	for {
		err := tcat.Next()
		if err == io.EOF {
			return empty, fmt.Errorf("%w: %q", ErrViewNotFound, tblname)
		}

		if err != nil {
			return empty, err
		}

		tname, err := tcat.Val(tableCatalogNameField)
		if err != nil {
			return empty, err
		}

		if tname.AsName().UnsafeAsGoString() == tblname {
			break
		}
	}

	// get the layout of the table from the fields catalog
	fcat := newTableScan(x, fieldsCatalogTableName, tm.fieldsCatalog)
	defer fcat.Close()

	schema := newSchema()
	// scan over the pages of the fields catalog
	// to look for fields belonging to the requested table.
	// Once found, build the layout
	for {
		err := fcat.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return empty, err
		}

		tname, err := fcat.Val(fieldsCatalogTableNameField)
		if err != nil {
			return empty, err
		}

		tbl := tname.AsName().UnsafeAsGoString()

		if tbl == tblname {
			// retrieve the field name, type, and index
			fldname, err := fcat.Val(fieldsCatalogNameField)
			if err != nil {
				return empty, err
			}

			fldtype, err := fcat.Val(fieldsCatalogTypeIDField)
			if err != nil {
				return empty, err
			}

			fldidx, err := fcat.Val(fieldsCatalogIndexField)
			if err != nil {
				return empty, err
			}

			schema.setFieldAtIndex(
				fldname.AsName().UnsafeAsGoString(),
				storage.FieldType(storage.ValueAsInteger[storage.SmallInt](fldtype)),
				storage.ValueAsInteger[storage.SmallInt](fldidx),
			)
		}
	}

	return NewLayout(schema), nil
}
