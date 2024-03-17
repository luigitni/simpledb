package meta

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/tx"
)

// NameMaxLen is the maximum len of a field or table name
const NameMaxLen = 16

var ErrNoViewFoud = errors.New("cannot find table in catalog")

type TableManager struct {
	// tcat is the catalog table for tables
	tcat record.Layout
	// fcat is the catalog table for fields
	fcat record.Layout
}

func NewTableManager() *TableManager {
	tcats := record.NewSchema()
	tcats.AddStringField("tblname", NameMaxLen)
	tcats.AddIntField("slotsize")
	tcat := record.NewLayout(tcats)

	fcats := record.NewSchema()
	fcats.AddStringField("tblname", NameMaxLen)
	fcats.AddStringField("fldname", NameMaxLen)
	fcats.AddIntField("type")
	fcats.AddIntField("length")
	fcats.AddIntField("offset")
	fcat := record.NewLayout(fcats)

	return &TableManager{
		tcat: tcat,
		fcat: fcat,
	}
}

func (tm TableManager) Init(trans tx.Transaction) {
	tm.CreateTable("tblcat", *tm.tcat.Schema(), trans)
	tm.CreateTable("fldcat", *tm.tcat.Schema(), trans)
}

func (tm TableManager) CreateTable(tblname string, sch record.Schema, tr tx.Transaction) error {
	layout := record.NewLayout(sch)

	tcat := record.NewTableScan(tr, "tblcat", tm.tcat)
	tcat.Insert()
	if err := tcat.SetString("tblname", tblname); err != nil {
		return err
	}

	if err := tcat.SetInt("slotsize", layout.SlotSize()); err != nil {
		return err
	}

	tcat.Close()

	// for each schema field, insert a record into the field catalog
	fcat := record.NewTableScan(tr, "fldcat", tm.fcat)
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

func (tm TableManager) Layout(tblname string, trans tx.Transaction) (record.Layout, error) {

	var empty record.Layout

	size := -1
	tcat := record.NewTableScan(trans, "tblcat", tm.tcat)
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
		return empty, fmt.Errorf("%w: %q", ErrNoViewFoud, tblname)
	}

	schema := record.NewSchema()
	offsets := map[string]int{}
	fcat := record.NewTableScan(trans, "fldcat", tm.fcat)
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
			schema.AddField(fldname, record.FieldType(fldtype), fldlen)
		}
	}

	fcat.Close()
	return record.NewLayoutFromMetadata(schema, offsets, size), nil
}
