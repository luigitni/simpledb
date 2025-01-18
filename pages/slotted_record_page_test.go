package pages

import (
	"testing"

	"github.com/luigitni/simpledb/types"
)

type mockLayout struct {
	indexes map[string]int
	sizes   map[string]types.Size
}

func (m mockLayout) FieldIndex(fname string) int {
	return m.indexes[fname]
}

func (m mockLayout) FieldsCount() int {
	return len(m.indexes)
}

func (m mockLayout) FieldSize(fname string) types.Size {
	return m.sizes[fname]
}

var _ Layout = mockLayout{}

func TestSlottedPageHeaderEntry(t *testing.T) {
	const (
		offset = 5
		length = 1024
		flag   = flagInUseRecord
	)

	var e slottedRecordPageHeaderEntry
	e = e.setOffset(offset)
	e = e.setLength(length)
	e = e.setFlag(flag)

	if v := e.recordOffset(); v != offset {
		t.Errorf("expected set offset to return %d, got %d", offset, v)
	}

	if v := e.recordLength(); v != length {
		t.Errorf("expected set length to return %d, got %d", length, v)
	}

	if v := e.flags(); v != flag {
		t.Errorf("expected set flag to return %d, got %d", flag, v)
	}
}

func TestSlottedRecordPageAppendRecordSlot(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedRecordPage(tx, types.NewBlock("file", 1), layout)
	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	t.Run("record is too large", func(t *testing.T) {
		if err := header.appendRecordSlot(defaultFreeSpaceEnd + 1); err != errNoFreeSpaceAvailable {
			t.Errorf("expected errNoFreeSpaceAvailabe, got %v", err)
		}
	})

	t.Run("successfully appends records", func(t *testing.T) {
		var recordsSize types.Offset
		for i, v := range []types.Offset{255, 1024, 512, 323, 8} {
			recordsSize += v

			if err := header.appendRecordSlot(v); err != nil {
				t.Fatalf("error appending record slot: %v", err)
			}

			got := header.entries[i]
			if got.recordLength() != v {
				t.Fatalf("exptected record size to be %d, got %d", v, got.recordLength())
			}

			if got.recordOffset() != defaultFreeSpaceEnd-recordsSize {
				t.Fatalf("expected offset to be %d, got %d", defaultFreeSpaceEnd-recordsSize, got.recordOffset())
			}

			if got.flags() != flagInUseRecord {
				t.Fatalf("expected flags to be %d, got %d", flagInUseRecord, got.flags())
			}
		}
	})
}

func TestSlottedRecordPageWriteHeader(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedRecordPage(tx, types.NewBlock("file", 1), layout)
	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully appends records", func(t *testing.T) {
		var recordsSize types.Offset
		for i, v := range []types.Offset{255, 1024, 512, 323, 8} {
			recordsSize += v

			header, err := page.readHeader()
			if err != nil {
				t.Fatalf("error reading header before write: %v", err)
			}

			if err := header.appendRecordSlot(v); err != nil {
				t.Fatalf("error appending record slot: %v", err)
			}

			if err := page.writeHeader(header); err != nil {
				t.Fatalf("error writing header: %v", err)
			}

			header, err = page.readHeader()
			if err != nil {
				t.Fatalf("error reading header after write: %v", err)
			}

			got := header.entries[i]
			if got.recordLength() != v {
				t.Fatalf("exptected record size to be %d, got %d", v, got.recordLength())
			}

			if got.recordOffset() != defaultFreeSpaceEnd-recordsSize {
				t.Fatalf("expected offset to be %d, got %d", defaultFreeSpaceEnd-recordsSize, got.recordOffset())
			}

			if got.flags() != flagInUseRecord {
				t.Fatalf("expected flags to be %d, got %d", flagInUseRecord, got.flags())
			}
		}
	})
}

func TestSlottedRecordPageReadHeader(t *testing.T) {
	tx := newMockTx()

	const (
		blockNumber  types.Long = 1
		freeSpaceEnd            = 1024
	)

	entries := []slottedRecordPageHeaderEntry{
		slottedRecordPageHeaderEntry{}.setOffset(0).setLength(256).setFlag(flagInUseRecord),
		slottedRecordPageHeaderEntry{}.setOffset(512).setLength(512).setFlag(flagEmptyRecord),
		slottedRecordPageHeaderEntry{}.setOffset(1024).setLength(1024).setFlag(flagInUseRecord),
	}

	page := NewSlottedRecordPage(tx, types.Block{}, nil)

	header := slottedRecordPageHeader{
		blockNumber:  blockNumber,
		numSlots:     types.SmallInt(len(entries)),
		freeSpaceEnd: freeSpaceEnd,
		entries:      entries,
	}

	if err := page.writeHeader(header); err != nil {
		t.Fatalf("error writing header: %v", err)
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatal(err)
	}

	if got := header.blockNumber; got != blockNumber {
		t.Errorf("expected block number %d, got %d", blockNumber, got)
	}

	if got := header.numSlots; got != types.SmallInt(len(entries)) {
		t.Errorf("expected numSlots %d, got %d", len(entries), got)
	}

	if got := header.freeSpaceEnd; got != freeSpaceEnd {
		t.Errorf("expected freeSpaceEnd %d, got %d", freeSpaceEnd, got)
	}

	for i, want := range entries {
		if got := header.entries[i]; got != want {
			t.Errorf("expected entry %d to be %v, got %v", i, want, got)
		}
	}
}

func TestSlottedRecordPageSearchAfter(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]types.Size{"field1": types.SizeOfInt},
	}

	page := NewSlottedRecordPage(tx, types.NewBlock("file", 1), layout)

	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("page is empty, no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchFrom(0, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	for i, v := range []types.Offset{255, 1024, 512, 323, 8} {
		if _, err := page.InsertFrom(types.SmallInt(i), v, false); err != nil {
			t.Fatalf("error appending record slot: %v", err)
		}
	}

	t.Run("no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchFrom(0, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	const slotToFree = 1

	if err := page.Delete(slotToFree); err != nil {
		t.Fatalf("error deleting record at slot %d: %v", slotToFree, err)
	}
}

func TestSlottedRecordPageInsertAfter(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedRecordPage(tx, types.NewBlock("file", 1), layout)

	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully inserts records one after another", func(t *testing.T) {
		for i, v := range []types.Offset{255, 1024, 512, 323, 8} {
			slot, err := page.InsertFrom(0, v, false)
			if err != nil {
				t.Errorf("error inserting after -1: %v", err)
			}

			if slot != types.SmallInt(i) {
				t.Errorf("expexted slot to be %d, got %d", i, slot)
			}
		}
	})

	t.Run("no space is availale for inserting records", func(t *testing.T) {
		_, err := page.InsertFrom(0, defaultFreeSpaceEnd+1, false)
		if err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})
}

func TestSlottedRecordPageSet(t *testing.T) {
	tx := newMockTx()

	layout := mockLayout{
		indexes: map[string]int{
			"field1": 0,
			"field2": 1,
			"field3": 2,
			"field4": 3,
		},
		sizes: map[string]types.Size{
			"field1": types.SizeOfTinyInt,
			"field2": 0,
			"field3": types.SizeOfInt,
			"field4": 0,
		},
	}

	page := NewSlottedRecordPage(tx, types.NewBlock("file", 1), layout)
	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	/*
	* suppose the record is the following:
	* {
	*   "field1": 12,
	*   "field2": "This is a variable string",
	*   "field3": 4567890
	*   "field3": "This is another string"
	*
	 */
	record := []interface{}{
		types.TinyInt(12), "This is a variable string", types.Int(4567890), "This is another string",
	}

	catalog := []string{"field1", "field2", "field3", "field4"}

	var recordLength types.Offset
	for i, v := range record {
		column := catalog[i]

		switch val := v.(type) {
		case types.TinyInt, types.Int:
			recordLength += types.Offset(layout.indexes[column])
		case string:
			recordLength += types.Offset(types.UnsafeSizeOfStringAsVarlen(val))
		default:
			t.Fatal("unsupported type")
		}
	}

	slot, err := page.InsertFrom(0, recordLength, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := page.SetFixedLen(
		slot,
		"field1",
		types.UnsafeIntegerToFixed(layout.sizes["field1"], record[0].(types.TinyInt)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field2",
		types.UnsafeNewVarlenFromGoString(record[1].(string)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetFixedLen(
		slot,
		"field3",
		types.UnsafeIntegerToFixed(layout.sizes["field3"], record[2].(types.Int)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field4",
		types.UnsafeNewVarlenFromGoString(record[3].(string)),
	); err != nil {
		t.Fatal(err)
	}

	gotFixed, err := page.FixedLen(slot, "field1")
	if err != nil {
		t.Fatal(err)
	}

	if v := types.UnsafeFixedToInteger[types.TinyInt](gotFixed); v != record[0].(types.TinyInt) {
		t.Errorf("expected field1 value to be %d, got %d", record[0], v)
	}

	gotVarlen, err := page.VarLen(slot, "field2")
	if err != nil {
		t.Fatal(err)
	}

	if v := types.UnsafeVarlenToGoString(gotVarlen); v != record[1].(string) {
		t.Errorf("expected field2 value to be %s, got %s", record[1], v)
	}

	gotFixed, err = page.FixedLen(slot, "field3")
	if err != nil {
		t.Fatal(err)
	}

	if v := types.UnsafeFixedToInteger[types.Int](gotFixed); v != record[2].(types.Int) {
		t.Errorf("expected field3 value to be %d, got %d", record[2], v)
	}

	gotVarlen, err = page.VarLen(slot, "field4")
	if err != nil {
		t.Fatal(err)
	}

	if v := types.UnsafeVarlenToGoString(gotVarlen); v != record[3].(string) {
		t.Errorf("expected field4 value to be %s, got %s", record[3], v)
	}
}
