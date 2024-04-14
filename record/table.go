package record

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

// NameMaxLen is the maximum len of a field or table name
const NameMaxLen = 16

var ErrViewNotFound = errors.New("cannot find table in catalog")

type TableManager struct {
	// tcat is the catalog table for tables
	tcat Layout
	// fcat is the catalog table for fields
	fcat Layout
}

func NewTableManager() *TableManager {
	tcats := NewSchema()
	tcats.AddStringField("tblname", NameMaxLen)
	tcats.AddIntField("slotsize")
	tcat := NewLayout(tcats)

	fcats := NewSchema()
	fcats.AddStringField("tblname", NameMaxLen)
	fcats.AddStringField("fldname", NameMaxLen)
	fcats.AddIntField("type")
	fcats.AddIntField("length")
	fcats.AddIntField("offset")
	fcat := NewLayout(fcats)

	return &TableManager{
		tcat: tcat,
		fcat: fcat,
	}
}

func (tm TableManager) Init(trans tx.Transaction) {
	tm.CreateTable("tblcat", *tm.tcat.Schema(), trans)
	tm.CreateTable("fldcat", *tm.tcat.Schema(), trans)
}

func (tm TableManager) TableExists(tblname string, tr tx.Transaction) bool {

	tcat := NewTableScan(tr, "tblcat", tm.tcat)

	q := "SELECT tblname FROM tblcat WHERE tblname = " + tblname
	p := sql.NewParser(q)
	data, err := p.Query()
	if err != nil {
		panic(err)
	}

	sel := NewSelectScan(tcat, data.Predicate())
	for {
		err := sel.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		v, err := sel.GetString("tblname")
		if err != nil {
			panic(err)
		}

		if v == tblname {
			return true
		}
	}

	return false
}

func (tm TableManager) CreateTable(tblname string, sch Schema, tr tx.Transaction) error {
	layout := NewLayout(sch)

	tcat := NewTableScan(tr, "tblcat", tm.tcat)
	tcat.Insert()
	if err := tcat.SetString("tblname", tblname); err != nil {
		return err
	}

	if err := tcat.SetInt("slotsize", layout.SlotSize()); err != nil {
		return err
	}

	tcat.Close()

	// for each schema field, insert a record into the field catalog
	fcat := NewTableScan(tr, "fldcat", tm.fcat)
	for _, fname := range sch.Fields() {
		fcat.Insert()
		if err := fcat.SetString("tblname", tblname); err != nil {
			return err
		}

		if err := fcat.SetString("fldname", fname); err != nil {
			return err
		}

		if err := fcat.SetInt("type", int(sch.Type(fname))); err != nil {
			return err
		}

		if err := fcat.SetInt("length", sch.Length(fname)); err != nil {
			return err
		}

		if err := fcat.SetInt("offset", layout.Offset(fname)); err != nil {
			return err
		}
	}
	fcat.Close()
	return nil
}

func (tm TableManager) Layout(tblname string, trans tx.Transaction) (Layout, error) {

	var empty Layout

	size := -1
	tcat := NewTableScan(trans, "tblcat", tm.tcat)
	for {
		err := tcat.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return empty, err
		}

		tname, err := tcat.GetString("tblname")
		if err != nil {
			return empty, err
		}

		if tname == tblname {
			size, err = tcat.GetInt("slotsize")
			if err != nil {
				return empty, err
			}
			break
		}
	}
	tcat.Close()

	if size < 0 {
		// could not find the table in the catalogue
		return empty, fmt.Errorf("%w: %q", ErrViewNotFound, tblname)
	}

	schema := NewSchema()
	offsets := map[string]int{}
	fcat := NewTableScan(trans, "fldcat", tm.fcat)
	for {
		err := fcat.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return empty, err
		}

		tname, err := fcat.GetString("tblname")
		if err != nil {
			return empty, err
		}

		if tname == tblname {
			fldname, err := fcat.GetString("fldname")
			if err != nil {
				return empty, err
			}

			fldtype, err := fcat.GetInt("type")
			if err != nil {
				return empty, err
			}

			fldlen, err := fcat.GetInt("length")
			if err != nil {
				return empty, err
			}

			offset, err := fcat.GetInt("offset")
			if err != nil {
				return empty, err
			}

			offsets[fldname] = offset
			schema.AddField(fldname, file.FieldType(fldtype), fldlen)
		}
	}

	fcat.Close()
	return NewLayoutFromMetadata(schema, offsets, size), nil
}
