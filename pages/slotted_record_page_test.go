package pages

import (
	"testing"

	"github.com/luigitni/simpledb/file"
)

type mockLayout struct {
	indexes map[string]int
}

func (m mockLayout) FieldIndex(fname string) int {
	return m.indexes[fname]
}

func (m mockLayout) FieldsCount() int {
	return len(m.indexes)
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

	page := NewSlottedRecordPage(tx, file.NewBlock("file", 1), layout)
	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	t.Run("record is too large", func(t *testing.T) {
		if err := header.appendRecordSlot(defaultFreeSpaceEnd + 1); err != errNoFreeSpaceAvailabe {
			t.Errorf("expected errNoFreeSpaceAvailabe, got %v", err)
		}
	})

	t.Run("successfully appends records", func(t *testing.T) {
		recordsSize := 0
		for i, v := range []int{255, 1024, 512, 323, 8} {
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

	page := NewSlottedRecordPage(tx, file.NewBlock("file", 1), layout)
	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully appends records", func(t *testing.T) {
		recordsSize := 0
		for i, v := range []int{255, 1024, 512, 323, 8} {
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
		blockNumber  = 1
		freeSpaceEnd = 1024
	)

	entries := []slottedRecordPageHeaderEntry{
		slottedRecordPageHeaderEntry(0).setOffset(0).setLength(256).setFlag(flagInUseRecord),
		slottedRecordPageHeaderEntry(0).setOffset(512).setLength(512).setFlag(flagEmptyRecord),
		slottedRecordPageHeaderEntry(0).setOffset(1024).setLength(1024).setFlag(flagInUseRecord),
	}

	page := NewSlottedRecordPage(tx, file.Block{}, nil)

	tx.SetInt(page.block, blockNumberOffset, blockNumber, false)
	tx.SetInt(page.block, numSlotsOffset, len(entries), false)
	tx.SetInt(page.block, freeSpaceEndOffset, freeSpaceEnd, false)
	for i, entry := range entries {
		tx.SetInt(page.block, entriesOffset+i*headerEntrySize, int(entry), false)
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatal(err)
	}

	if got := header.blockNumber; got != blockNumber {
		t.Errorf("expected block number %d, got %d", blockNumber, got)
	}

	if got := header.numSlots; got != len(entries) {
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
	}

	page := NewSlottedRecordPage(tx, file.NewBlock("file", 1), layout)

	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	t.Run("page is empty, no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchAfter(0, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	for _, v := range []int{255, 1024, 512, 323, 8} {
		if err := header.appendRecordSlot(v); err != nil {
			t.Fatalf("error appending record slot: %v", err)
		}
	}

	if err := page.writeHeader(header); err != nil {
		t.Fatalf("error writing header: %v", err)
	}

	t.Run("no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchAfter(0, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	const slotToFree = 1

	if err := page.Delete(slotToFree); err != nil {
		t.Fatalf("error deleting record at slot %d: %v", slotToFree, err)
	}

	t.Run("successfully finds free slot after deletion of existing record", func(t *testing.T) {
		slot, err := page.searchAfter(0, flagEmptyRecord, 1024)
		if err != nil {
			t.Fatalf("error searching after: %v", err)
		}

		if slot != slotToFree {
			t.Errorf("expected slot %d, got %d", slotToFree, slot)
		}
	})
}

func TestSlottedRecordPageInsertAfter(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedRecordPage(tx, file.NewBlock("file", 1), layout)

	if err := page.Format(); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully inserts records one after another", func(t *testing.T) {
		for i, v := range []int{255, 1024, 512, 323, 8} {
			slot, err := page.InsertAfter(i-1, v)
			if err != nil {
				t.Errorf("error inserting after -1: %v", err)
			}

			if slot != i {
				t.Errorf("expexted slot to be %d, got %d", i, slot)
			}
		}
	})

	t.Run("no space is availale for inserting records", func(t *testing.T) {
		_, err := page.InsertAfter(-1, defaultFreeSpaceEnd+1)
		if err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	page.Delete(2)

	t.Run("successfully inserts record after deleting existing one", func(t *testing.T) {
		slot, err := page.InsertAfter(1, 500)
		if err != nil {
			t.Fatalf("error inserting after 1: %v", err)
		}

		if slot != 2 {
			t.Errorf("expected slot to be 2, got %d", slot)
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
	}

	page := NewSlottedRecordPage(tx, file.NewBlock("file", 1), layout)
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
		12, "This is a variable string", 4567890, "This is another string",
	}

	recordLenght := 0
	for _, v := range record {
		switch val := v.(type) {
		case int:
			recordLenght += file.IntSize
		case string:
			l := file.StrLength(len(val))
			recordLenght += l
		default:
			t.Fatal("unsupported type")
		}
	}

	slot, err := page.InsertAfter(-1, recordLenght)
	if err != nil {
		t.Fatal(err)
	}

	if err := page.SetInt(slot, "field1", record[0].(int)); err != nil {
		t.Fatal(err)
	}

	if err := page.SetString(slot, "field2", record[1].(string)); err != nil {
		t.Fatal(err)
	}

	if err := page.SetInt(slot, "field3", record[2].(int)); err != nil {
		t.Fatal(err)
	}

	if err := page.SetString(slot, "field4", record[3].(string)); err != nil {
		t.Fatal(err)
	}

	gotInt, err := page.Int(slot, "field1")
	if err != nil {
		t.Fatal(err)
	}

	if gotInt != record[0].(int) {
		t.Errorf("expected field1 value to be %d, got %d", record[0], gotInt)
	}

	gotStr, err := page.String(slot, "field2")
	if err != nil {
		t.Fatal(err)
	}

	if gotStr != record[1].(string) {
		t.Errorf("expected field2 value to be %s, got %s", record[1], gotStr)
	}

	gotInt, err = page.Int(slot, "field3")
	if err != nil {
		t.Fatal(err)
	}

	if gotInt != record[2].(int) {
		t.Errorf("expected field3 value to be %d, got %d", record[2], gotInt)
	}

	gotStr, err = page.String(slot, "field4")
	if err != nil {
		t.Fatal(err)
	}

	if gotStr != record[3].(string) {
		t.Errorf("expected field4 value to be %s, got %s", record[3], gotStr)
	}
}
