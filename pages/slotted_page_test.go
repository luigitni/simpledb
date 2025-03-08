package pages

import (
	"fmt"
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

type mockLayout struct {
	indexes map[string]int
	sizes   map[string]storage.Offset
}

func (m mockLayout) FieldIndex(fname string) int {
	return m.indexes[fname]
}

func (m mockLayout) FieldsCount() int {
	return len(m.indexes)
}

func (m mockLayout) FieldSize(fname string) storage.Offset {
	return m.sizes[fname]
}

func (m mockLayout) FieldSizeByIndex(idx int) storage.Offset {
	for k, v := range m.indexes {
		if v == idx {
			return m.sizes[k]
		}
	}

	return 0
}

var _ Layout = mockLayout{}

func TestSlottedPageHeaderEntry(t *testing.T) {
	const (
		offset = 5
		length = 1024
		flag   = flagInUseRecord
	)

	var e slottedPageHeaderEntry
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

func TestSlottedPageAppendRecordSlot(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	page := NewSlottedPage(x, block, layout)
	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	header, err := page.Header()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	t.Run("record is too large", func(t *testing.T) {
		if err := header.appendRecordSlot(defaultFreeSpaceEnd + 1); err != errNoFreeSpaceAvailable {
			t.Errorf("expected errNoFreeSpaceAvailabe, got %v", err)
		}
	})

	t.Run("successfully appends records", func(t *testing.T) {
		var recordsSize storage.Offset
		for i, v := range []storage.Offset{255, 1024, 512, 323, 8} {
			recordsSize += v

			if err := header.appendRecordSlot(v); err != nil {
				t.Fatalf("error appending record slot: %v", err)
			}

			got, err := header.entry(storage.SmallInt(i))
			if err != nil {
				t.Fatal(err)
			}

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

func TestSlottedPageWriteHeader(t *testing.T) {

	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	page := NewSlottedPage(x, block, layout)
	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully appends records", func(t *testing.T) {
		var recordsSize storage.Offset
		for i, v := range []storage.Offset{255, 1024, 512, 323, 8} {
			recordsSize += v

			header, err := page.Header()
			if err != nil {
				t.Fatalf("error reading header before write: %v", err)
			}

			if err := header.appendRecordSlot(v); err != nil {
				t.Fatalf("error appending record slot: %v", err)
			}

			got, err := header.entry(storage.SmallInt(i))
			if err != nil {
				t.Fatal(err)
			}

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

func TestSlottedPageReadHeader(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	const (
		blockNumber  storage.Long   = 1
		freeSpaceEnd storage.Offset = 1024
	)

	block := storage.NewBlock(test.RandomName(), blockNumber)
	x.Append(block.FileName())

	entries := []slottedPageHeaderEntry{
		slottedPageHeaderEntry{}.setOffset(0).setLength(256).setFlag(flagInUseRecord),
		slottedPageHeaderEntry{}.setOffset(512).setLength(512).setFlag(flagEmptyRecord),
		slottedPageHeaderEntry{}.setOffset(1024).setLength(1024).setFlag(flagInUseRecord),
	}

	page := NewSlottedPage(x, block, nil)
	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	header, err := page.Header()
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		header.appendEntry(e)
	}

	if got := header.mustBlockNumber(); got != blockNumber {
		t.Errorf("expected block number %d, got %d", blockNumber, got)
	}

	if got := header.mustNumSlots(); got != storage.SmallInt(len(entries)) {
		t.Errorf("expected numSlots %d, got %d", len(entries), got)
	}

	if got := header.mustFreeSpaceEnd(); got != freeSpaceEnd {
		t.Errorf("expected freeSpaceEnd %d, got %d", freeSpaceEnd, got)
	}

	for i, want := range entries {
		got, err := header.entry(storage.SmallInt(i))
		if err != nil {
			t.Fatalf("error getting entry %d: %v", i, err)
		}

		if got != want {
			t.Errorf("expected entry %d to be %v, got %v", i, want, got)
		}
	}
}

func TestSlottedPageSearchAfter(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfInt},
	}

	page := NewSlottedPage(x, storage.NewBlock("file", 1), layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("page is empty, no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchAfter(BeforeFirstSlot, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	for i, v := range []storage.Offset{255, 1024, 512, 323, 8} {
		if _, err := page.InsertAfter(storage.SmallInt(i), v, false); err != nil {
			t.Fatalf("error appending record slot: %v", err)
		}
	}

	t.Run("no free slot available amongst the existing ones", func(t *testing.T) {
		if _, err := page.searchAfter(BeforeFirstSlot, flagEmptyRecord, 1024); err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})

	const slotToFree = 1

	if err := page.Delete(slotToFree); err != nil {
		t.Fatalf("error deleting record at slot %d: %v", slotToFree, err)
	}
}

func TestSlottedPageInsertAfter(t *testing.T) {

	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedPage(x, storage.NewBlock("file", 1), layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("successfully inserts records one after another", func(t *testing.T) {
		for i, v := range []storage.Offset{255, 1024, 512, 323, 8} {
			slot, err := page.InsertAfter(BeforeFirstSlot, v, false)
			if err != nil {
				t.Errorf("error inserting after -1: %v", err)
			}

			if slot != storage.SmallInt(i) {
				t.Errorf("expexted slot to be %d, got %d", i, slot)
			}
		}
	})

	t.Run("no space is availale for inserting records", func(t *testing.T) {
		_, err := page.InsertAfter(BeforeFirstSlot, defaultFreeSpaceEnd+1, false)
		if err != ErrNoFreeSlot {
			t.Errorf("expected ErrNoFreeSlot, got %v", err)
		}
	})
}

func TestSlottedPageSet(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock("set_page", 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{
			"field1": 0,
			"field2": 1,
			"field3": 2,
			"field4": 3,
		},
		sizes: map[string]storage.Offset{
			"field1": storage.SizeOfTinyInt,
			"field2": storage.SizeOfVarlen,
			"field3": storage.SizeOfInt,
			"field4": storage.SizeOfVarlen,
		},
	}

	page := NewSlottedPage(x, storage.NewBlock("file", 1), layout)
	if err := page.Format(0); err != nil {
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
		storage.TinyInt(12), "This is a variable string", storage.Int(4567890), "This is another string",
	}

	catalog := []string{"field1", "field2", "field3", "field4"}

	var recordLength storage.Offset
	for i, v := range record {
		column := catalog[i]

		switch val := v.(type) {
		case storage.TinyInt, storage.Int:
			recordLength += storage.Offset(layout.indexes[column])
		case string:
			recordLength += storage.Offset(storage.SizeOfStringAsVarlen(val))
		default:
			t.Fatal("unsupported type")
		}
	}

	slot, err := page.InsertAfter(BeforeFirstSlot, recordLength, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := page.SetFixedLen(
		slot,
		"field1",
		storage.IntegerToFixedLen(layout.sizes["field1"], record[0].(storage.TinyInt)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field2",
		storage.NewVarlenFromGoString(record[1].(string)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetFixedLen(
		slot,
		"field3",
		storage.IntegerToFixedLen(layout.sizes["field3"], record[2].(storage.Int)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field4",
		storage.NewVarlenFromGoString(record[3].(string)),
	); err != nil {
		t.Fatal(err)
	}

	gotFixed, err := page.FixedLen(slot, "field1")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.FixedLenToInteger[storage.TinyInt](gotFixed); v != record[0].(storage.TinyInt) {
		t.Errorf("expected field1 value to be %d, got %d", record[0], v)
	}

	gotVarlen, err := page.VarLen(slot, "field2")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.VarlenToGoString(gotVarlen); v != record[1].(string) {
		t.Errorf("expected field2 value to be %s, got %s", record[1], v)
	}

	gotFixed, err = page.FixedLen(slot, "field3")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.FixedLenToInteger[storage.Int](gotFixed); v != record[2].(storage.Int) {
		t.Errorf("expected field3 value to be %d, got %d", record[2], v)
	}

	gotVarlen, err = page.VarLen(slot, "field4")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.VarlenToGoString(gotVarlen); v != record[3].(string) {
		t.Errorf("expected field4 value to be %s, got %s", record[3], v)
	}
}

func TestSlottedPageSetAtSpecial(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	const specialSpaceSize storage.Offset = 512

	t.Run("format header includes special space size", func(t *testing.T) {
		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 1)
		x.Append(block.FileName())

		page := NewSlottedPage(x, block, nil)

		if err := page.Format(specialSpaceSize); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		header, err := page.Header()
		if err != nil {
			t.Fatalf("error reading header: %v", err)
		}

		if got := header.mustBlockNumber(); got != 1 {
			t.Fatalf("expected block number to be 1, got %d", got)
		}

		if got := header.mustNumSlots(); got != 0 {
			t.Fatalf("expected num slots to be 0, got %d", got)
		}

		if got := header.mustFreeSpaceEnd(); got != defaultFreeSpaceEnd-specialSpaceSize {
			t.Fatalf("expected free space end to be %d, got %d", defaultFreeSpaceEnd-specialSpaceSize, got)
		}

		if got := header.mustSpecialSpaceStart(); got != defaultFreeSpaceEnd-specialSpaceSize {
			t.Fatalf("expected special space start to be %d, got %d", defaultFreeSpaceEnd-specialSpaceSize, got)
		}
	})

	t.Run("set fixedlen at special slot", func(t *testing.T) {
		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		page := NewSlottedPage(x, block, nil)

		if err := page.Format(specialSpaceSize); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		const exp storage.Int = 12345
		f := storage.IntegerToFixedLen[storage.Int](storage.SizeOfInt, exp)

		if err := page.SetFixedLenAtSpecial(0, storage.SizeOfInt, f); err != nil {
			t.Fatalf("error setting fixedlen at special slot: %v", err)
		}

		got, err := page.FixedLenAtSpecial(0, storage.SizeOfInt)
		if err != nil {
			t.Fatalf("error getting fixedlen at special slot: %v", err)
		}

		if v := storage.FixedLenToInteger[storage.Int](got); v != exp {
			t.Errorf("expected value to be %d, got %d", exp, v)
		}
	})

	t.Run("set varlen at special slot", func(t *testing.T) {
		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		page := NewSlottedPage(x, block, nil)

		if err := page.Format(specialSpaceSize); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		const exp = "This is a string"
		v := storage.NewVarlenFromGoString(exp)

		if err := page.SetVarLenAtSpecial(0, v); err != nil {
			t.Fatalf("error setting varlen at special slot: %v", err)
		}

		got, err := page.VarLenAtSpecial(0)
		if err != nil {
			t.Fatalf("error getting varlen at special slot: %v", err)
		}

		if v := storage.VarlenToGoString(got); v != exp {
			t.Errorf("expected value to be %s, got %s", exp, v)
		}
	})

}

func TestSlottedPageDelete(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	page := NewSlottedPage(x, block, layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("delete record at slot", func(t *testing.T) {
		slot, err := page.InsertAfter(BeforeFirstSlot, 1024, false)
		if err != nil {
			t.Fatalf("error inserting record: %v", err)
		}

		if err := page.Delete(slot); err != nil {
			t.Fatalf("error deleting record: %v", err)
		}

		entry, err := page.entry(slot)
		if err != nil {
			t.Fatalf("error getting entry: %v", err)
		}

		if entry.flags() != flagDeletedRecord {
			t.Errorf("expected flag to be %d, got %d", flagDeletedRecord, entry.flags())
		}
	})
}

func TestSlottedPageInsertAt(t *testing.T) {

	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock("shift_right", 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	page := NewSlottedPage(x, block, layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	const numRecords = 100
	for i := 0; i < numRecords; i++ {
		slot := storage.SmallInt(i)
		err := page.InsertAt(slot, storage.SizeOfSmallInt)
		if err != nil {
			t.Fatalf("error inserting record: %v", err)
		}

		f := storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(i))
		if err := page.SetFixedLen(slot, "field1", f); err != nil {
			t.Fatalf("error setting fixedlen: %v", err)
		}

		header, err := page.Header()
		if err != nil {
			t.Fatalf("error reading header: %v", err)
		}

		if got := header.mustNumSlots(); got != storage.SmallInt(i+1) {
			t.Fatalf("expected num slots to be %d, got %d at iteration %d", i+1, got, i)
		}

		expectedFreeSpaceEnd := defaultFreeSpaceEnd - storage.SizeOfSmallInt*storage.Offset(i+1)
		if got := header.mustFreeSpaceEnd(); got != expectedFreeSpaceEnd {
			t.Fatalf("expected free space end to be %d, got %d at iteration %d", expectedFreeSpaceEnd, got, i)
		}
	}

	if err := page.InsertAt(50, storage.SizeOfSmallInt); err != nil {
		t.Fatalf("error shifting slots right: %v", err)
	}

	f := storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(999))
	if err := page.SetFixedLen(50, "field1", f); err != nil {
		t.Fatalf("error setting fixedlen: %v", err)
	}

	test := func(i, exp storage.SmallInt) error {
		v, err := page.FixedLen(i, "field1")
		if err != nil {
			return err
		}

		if got := storage.FixedLenToInteger[storage.SmallInt](v); got != exp {
			return fmt.Errorf("expected %d, got %d", exp, got)
		}

		return nil
	}

	header, err := page.Header()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	slots := int(header.mustNumSlots())
	if slots != 101 {
		t.Fatalf("expected num slots to be 101, got %d", slots)
	}

	// test that before the pivot, the records are in the correct position
	for i := 0; i < 50; i++ {
		if err := test(storage.SmallInt(i), storage.SmallInt(i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := test(storage.SmallInt(50), storage.SmallInt(999)); err != nil {
		t.Fatal(err)
	}

	// test that after the pivot, the records are in shifted to the right
	for i := 51; i < slots; i++ {
		if err := test(storage.SmallInt(i), storage.SmallInt(i-1)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSlottedPageShiftSlotsLeft(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock("shift_left", 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	page := NewSlottedPage(x, storage.NewBlock("file", 1), layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	const numRecords = 100
	slot := BeforeFirstSlot
	for i := 0; i < numRecords; i++ {
		slot, err := page.InsertAfter(slot, storage.SizeOfSmallInt, false)
		if err != nil {
			t.Fatalf("error inserting record: %v", err)
		}

		f := storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(i))
		if err := page.SetFixedLen(slot, "field1", f); err != nil {
			t.Fatalf("error setting fixedlen: %v", err)
		}
	}

	// ShiftSlotsLeft will shift the records to the left by 1 slot
	// starting from the pivot slot, which is 50 in this case.
	// Record 50 will be deleted and records from 51 to 99 will be shifted to the left by 1 slot:
	// Record 51 will be moved to slot 50, record 52 will be moved to slot 51, and so on.
	if err := page.ShiftSlotsLeft(50); err != nil {
		t.Fatalf("error shifting slots left: %v", err)
	}

	test := func(i, exp storage.SmallInt) error {
		v, err := page.FixedLen(i, "field1")
		if err != nil {
			return err
		}

		if got := storage.FixedLenToInteger[storage.SmallInt](v); got != exp {
			return fmt.Errorf("expected %d at slot %d, got %d", exp, i, got)
		}

		return nil
	}

	header, err := page.Header()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	slots := int(header.mustNumSlots())
	if slots != 99 {
		t.Fatalf("expected num slots to be 99, got %d", slots)
	}

	// test that before the pivot, the records are in the correct position
	for i := 0; i < 50; i++ {
		if err := test(storage.SmallInt(i), storage.SmallInt(i)); err != nil {
			t.Fatal(err)
		}
	}

	// test that after the pivot, the records are in shifted to the left
	for i := 50; i < slots; i++ {
		slot := storage.SmallInt(i)
		exp := storage.SmallInt(i + 1)
		if err := test(slot, exp); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSlottedPageCompact(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	t.Run("compact page", func(t *testing.T) {
		tx := tx.NewTx(fm, lm, bm)
		defer tx.Commit()

		block := storage.NewBlock("compact_page", 0)
		tx.Append(block.FileName())

		layout := mockLayout{
			indexes: map[string]int{"field1": 0},
			sizes:   map[string]storage.Offset{"field1": storage.SizeOfInt},
		}

		page := NewSlottedPage(tx, block, layout)

		if err := page.Format(0); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		const (
			firstValue  storage.Int = 255
			secondValue storage.Int = 1023
			thirdValue  storage.Int = 999813
		)
		for i, v := range []storage.Int{firstValue, secondValue, thirdValue} {

			slot, err := page.InsertAfter(BeforeFirstSlot, storage.SizeOfInt, false)
			if err != nil {
				t.Fatalf("error inserting record: %v", err)
			}

			if slot != storage.SmallInt(i) {
				t.Fatalf("expected slot to be %d, got %d", i, slot)
			}

			f := storage.IntegerToFixedLen[storage.Int](storage.SizeOfInt, v)

			if err := page.SetFixedLen(slot, "field1", f); err != nil {
				t.Fatalf("error setting fixedlen: %v", err)
			}
		}

		recordSize := storage.SizeOfInt + recordHeaderSize

		// test that the first record is at offset 0 from the end.
		// test that the second record is at offset storage.SizeOfInt from the end.
		// test that the third record is at offset storage.SizeOfInt*2 from the end.
		header, err := page.Header()
		if err != nil {
			t.Fatalf("error reading header: %v", err)
		}

		for i, num := range []storage.Offset{1, 2, 3} {
			want := defaultFreeSpaceEnd - num*recordSize

			entry, err := header.entry(storage.SmallInt(i))
			if err != nil {
				t.Fatalf("error getting entry: %v", err)
			}

			if got := entry.recordOffset(); got != want {
				t.Fatalf("expected record offset to be %d, got %d", want, got)
			}
		}

		if got := header.mustFreeSpaceEnd(); got != defaultFreeSpaceEnd-3*recordSize {
			t.Fatalf("expected free space end to be %d, got %d", defaultFreeSpaceEnd-(3*recordSize), got)
		}

		// delete the middle record
		if err := page.Delete(1); err != nil {
			t.Fatalf("error deleting record: %v", err)
		}

		if err := page.Compact(); err != nil {
			t.Fatalf("error compacting page: %v", err)
		}

		// test that the first record is still at offset 0 from the end.
		// test the the third record is now at offset storage.SizeOfInt from the end.
		if err != nil {
			t.Fatalf("error reading header: %v", err)
		}

		if got := header.mustFreeSpaceEnd(); got != defaultFreeSpaceEnd-2*recordSize {
			t.Fatalf("expected free space end to be %d, got %d", defaultFreeSpaceEnd-(2*storage.SizeOfInt), got)
		}

		for slot, num := range map[storage.SmallInt]storage.Offset{
			0: 1,
			2: 2,
		} {
			want := defaultFreeSpaceEnd - num*recordSize
			entry, err := header.entry(slot)
			if err != nil {
				t.Fatalf("error getting entry: %v", err)
			}

			if got := entry.recordOffset(); got != want {
				t.Fatalf("expected record offset to be %d, got %d", want, got)
			}
		}

		// test the values are still the same
		for slot, want := range map[storage.SmallInt]storage.Int{
			0: firstValue,
			2: thirdValue,
		} {
			f, err := page.FixedLen(slot, "field1")
			if err != nil {
				t.Fatalf("error getting fixedlen: %v", err)
			}

			if got := storage.FixedLenToInteger[storage.Int](f); got != want {
				t.Fatalf("expected value at slot %d to be %d, got %d", slot, want, got)
			}
		}
	})

	t.Run("fill page with records and compact", func(t *testing.T) {
		layout := mockLayout{
			indexes: map[string]int{"field1": 0},
			sizes:   map[string]storage.Offset{"field1": storage.SizeOfInt},
		}

		tx := tx.NewTx(fm, lm, bm)
		defer tx.Commit()

		block := storage.NewBlock("fill_page_compact", 0)
		tx.Append(block.FileName())

		page := NewSlottedPage(tx, block, layout)

		if err := page.Format(0); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		for {
			slot, err := page.InsertAfter(BeforeFirstSlot, storage.SizeOfInt, false)
			if err == ErrNoFreeSlot {
				break
			}

			if err != nil {
				t.Fatalf("error inserting record: %v", err)
			}

			f := storage.IntegerToFixedLen[storage.Int](storage.SizeOfInt, storage.Int(slot))

			if err := page.SetFixedLen(slot, "field1", f); err != nil {
				t.Fatalf("error setting fixedlen: %v", err)
			}
		}

		// delete all records
		header, err := page.Header()
		if err != nil {
			t.Fatalf("error reading header: %v", err)
		}

		for i := range header.mustNumSlots() {
			if err := page.Delete(storage.SmallInt(i)); err != nil {
				t.Fatalf("error deleting record: %v", err)
			}
		}

		if err := page.Compact(); err != nil {
			t.Fatalf("error compacting page: %v", err)
		}

		if got := header.mustFreeSpaceEnd(); got != defaultFreeSpaceEnd {
			t.Fatalf("expected free space end to be %d, got %d", defaultFreeSpaceEnd, got)
		}

	})
}

func TestSlottedPageTruncate(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock("truncate", 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfSmallInt},
	}

	fc := layout.FieldsCount()
	if fc != 1 {
		t.Fatalf("expected fields count to be 1, got %d", fc)
	}

	page := NewSlottedPage(x, storage.NewBlock("file", 1), layout)

	if err := page.Format(0); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	const numRecords = 100
	slot := BeforeFirstSlot
	for i := 0; i < numRecords; i++ {
		slot, err := page.InsertAfter(slot, storage.SizeOfSmallInt, false)
		if err != nil {
			t.Fatalf("error inserting record: %v", err)
		}

		f := storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(i))
		if err := page.SetFixedLen(slot, "field1", f); err != nil {
			t.Fatalf("error setting fixedlen: %v", err)
		}
	}

	if err := page.Truncate(50); err != nil {
		t.Fatalf("error truncating page: %v", err)
	}

	header, err := page.Header()
	if err != nil {
		t.Fatalf("error reading header: %v", err)
	}

	got := header.mustNumSlots()
	if got != 50 {
		t.Fatalf("expected num slots to be 50, got %d", got)
	}

	// expect the start of the free space at the end of the last slot
	expectedFreeSpaceStart := 50*sizeOfPageHeaderEntry + entriesOffset
	if got := header.freeSpaceStart(); got != expectedFreeSpaceStart {
		t.Fatalf("expected free space start to be %d, got %d", expectedFreeSpaceStart, got)
	}

	// expect the end of the free space to be at the end of the last record
	expectedRecordSize := storage.SizeOfSmallInt + recordHeaderSize
	expectedFreeSpaceEnd := defaultFreeSpaceEnd - (50 * expectedRecordSize)
	if got := header.mustFreeSpaceEnd(); got != expectedFreeSpaceEnd {
		t.Fatalf("expected free space end to be %d, got %d", expectedFreeSpaceEnd, got)
	}

	for i := range got {
		f, err := page.FixedLen(storage.SmallInt(i), "field1")
		if err != nil {
			t.Fatalf("error getting fixedlen: %v", err)
		}

		if got := storage.FixedLenToInteger[storage.SmallInt](f); got != storage.SmallInt(i) {
			t.Fatalf("expected value to be %d, got %d", i, got)
		}
	}
}

func BenchmarkSlottedPageCompact(b *testing.B) {

	fm, lm, bm := test.MakeManagersWithDir(b.TempDir())

	x := tx.NewTx(fm, lm, bm)

	block := storage.NewBlock("benchmark", 0)
	x.Append(block.FileName())

	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Offset{"field1": storage.SizeOfInt},
	}

	x.Commit()

	preparePage := func(x tx.Transaction) *SlottedPage {

		page := NewSlottedPage(x, block, layout)

		if err := page.Format(0); err != nil {
			b.Fatalf("error formatting page: %v", err)
		}

		for {
			slot, err := page.InsertAfter(BeforeFirstSlot, storage.SizeOfInt, false)
			if err == ErrNoFreeSlot {

				break
			}

			if err != nil {
				b.Fatalf("error inserting record: %v", err)
			}

			f := storage.IntegerToFixedLen[storage.Long](storage.SizeOfInt, storage.Long(slot))
			if err := page.SetFixedLen(slot, "field1", f); err != nil {
				b.Fatalf("error setting fixedlen: %v", err)
			}
		}

		// delete all records
		header, err := page.Header()
		if err != nil {
			b.Fatalf("error reading header: %v", err)
		}

		for i := range header.mustNumSlots() {
			if err := page.Delete(storage.SmallInt(i)); err != nil {
				b.Fatalf("error deleting record: %v", err)
			}
		}

		return page
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {

		b.StopTimer()
		x := tx.NewTx(fm, lm, bm)
		p := preparePage(x)
		b.StartTimer()
		if err := p.Compact(); err != nil {
			b.Fatalf("error compacting page: %v", err)
		}

		b.StopTimer()
		x.Commit()
		b.StartTimer()
	}

}
