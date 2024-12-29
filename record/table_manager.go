package record

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

const (
	tableCatalogTableName = "tblcat"
	catFieldTableName     = "tblname"

	fieldsCatalogTableName = "fldcat"
	catFieldFieldName      = "fldname"
	catFieldType           = "type"
	catFieldIndex          = "fldidx"

	// NameMaxLen is the maximum len of a field or table name
	NameMaxLen = 16
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
	// tcat is the catalog table for tables
	tcat Layout
	// fcat is the catalog table for fields
	fcat Layout
}

func newTableManager() *tableManager {
	tcats := newSchema()
	tcats.addFixedLenStringField(catFieldTableName, NameMaxLen)
	tcat := NewLayout(tcats)

	fcats := newSchema()
	fcats.addFixedLenStringField(catFieldTableName, NameMaxLen)
	fcats.addFixedLenStringField(catFieldFieldName, NameMaxLen)
	fcats.addIntField(catFieldType)
	fcats.addIntField(catFieldIndex)
	fcat := NewLayout(fcats)

	return &tableManager{
		tcat: tcat,
		fcat: fcat,
	}
}

func (tm tableManager) init(x tx.Transaction) error {
	if err := tm.createTable(tableCatalogTableName, *tm.tcat.Schema(), x); err != nil {
		return err
	}

	if err := tm.createTable(fieldsCatalogTableName, *tm.fcat.Schema(), x); err != nil {
		return err
	}

	return nil
}

func (tm tableManager) tableExists(tblname string, tr tx.Transaction) bool {
	tcat := newTableScan(tr, tableCatalogTableName, tm.tcat)

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

		v, err := sel.String(catFieldTableName)
		if err != nil {
			panic(err)
		}

		if v == tblname {
			return true
		}
	}

	return false
}

// createTable takes the table's name and schema and defines the record format
// by calculating each fields offset.
// It adds the newly created table as a record into the table catalog file and then
// adds each field of the table to the field catalog.
func (tm tableManager) createTable(tblname string, sch Schema, x tx.Transaction) error {
	layout := NewLayout(sch)

	tcat := newTableScan(x, tableCatalogTableName, tm.tcat)
	defer tcat.Close()

	// add the new table into the table catalog
	size := file.StrLength(len(tblname))
	if err := tcat.Insert(size); err != nil {
		return err
	}

	if err := tcat.SetString(catFieldTableName, tblname); err != nil {
		return err
	}

	// for each schema field, insert a record into the field catalog.
	fcat := newTableScan(x, fieldsCatalogTableName, tm.fcat)
	defer fcat.Close()

	for _, fname := range sch.fields {
		size := 0
		size += file.StrLength(len(tblname))
		size += file.StrLength(len(fname))
		size += file.IntSize // fieldType
		size += file.IntSize // fieldIndex

		// scan up to the first available slot and add the field data to the field catalog
		if err := fcat.Insert(size); err != nil {
			return err
		}

		if err := fcat.SetString(catFieldTableName, tblname); err != nil {
			return err
		}

		if err := fcat.SetString(catFieldFieldName, fname); err != nil {
			return err
		}

		if err := fcat.SetInt(catFieldType, int(sch.ftype(fname))); err != nil {
			return err
		}

		if err := fcat.SetInt(catFieldIndex, layout.FieldIndex(fname)); err != nil {
			return err
		}
	}

	return nil
}

// layout opens two table scans, one into the table catalog table and the other one
// into the fields catalog, and retrieves the layout of the requested table.
func (tm tableManager) layout(tblname string, x tx.Transaction) (Layout, error) {
	var empty Layout

	tcat := newTableScan(x, tableCatalogTableName, tm.tcat)
	defer tcat.Close()
	for {
		err := tcat.Next()
		if err == io.EOF {
			return empty, fmt.Errorf("%w: %q", ErrViewNotFound, tblname)
		}

		if err != nil {
			return empty, err
		}

		tname, err := tcat.String(catFieldTableName)
		if err != nil {
			return empty, err
		}

		if tname == tblname {
			break
		}
	}

	fcat := newTableScan(x, fieldsCatalogTableName, tm.fcat)
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

		tname, err := fcat.String(catFieldTableName)
		if err != nil {
			return empty, err
		}

		if tname == tblname {
			fldname, err := fcat.String(catFieldFieldName)
			if err != nil {
				return empty, err
			}

			fldtype, err := fcat.Int(catFieldType)
			if err != nil {
				return empty, err
			}

			fldidx, err := fcat.Int(catFieldIndex)
			if err != nil {
				return empty, err
			}

			schema.setFieldAtIndex(fldname, file.FieldType(fldtype), fldidx)
		}
	}

	return NewLayout(schema), nil
}
