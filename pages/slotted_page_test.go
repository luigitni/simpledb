package pages

import (
	"testing"

	"github.com/luigitni/simpledb/storage"
)

type mockLayout struct {
	indexes map[string]int
	sizes   map[string]storage.Size
}

func (m mockLayout) FieldIndex(fname string) int {
	return m.indexes[fname]
}

func (m mockLayout) FieldsCount() int {
	return len(m.indexes)
}

func (m mockLayout) FieldSize(fname string) storage.Size {
	return m.sizes[fname]
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
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)
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
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)
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
	tx := newMockTx()

	const (
		blockNumber  storage.Long   = 1
		freeSpaceEnd storage.Offset = 1024
	)

	entries := []slottedPageHeaderEntry{
		slottedPageHeaderEntry{}.setOffset(0).setLength(256).setFlag(flagInUseRecord),
		slottedPageHeaderEntry{}.setOffset(512).setLength(512).setFlag(flagEmptyRecord),
		slottedPageHeaderEntry{}.setOffset(1024).setLength(1024).setFlag(flagInUseRecord),
	}

	page := NewSlottedPage(tx, storage.NewBlock("testfile", 1), nil)
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
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
		sizes:   map[string]storage.Size{"field1": storage.SizeOfInt},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

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
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

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
	tx := newMockTx()

	layout := mockLayout{
		indexes: map[string]int{
			"field1": 0,
			"field2": 1,
			"field3": 2,
			"field4": 3,
		},
		sizes: map[string]storage.Size{
			"field1": storage.SizeOfTinyInt,
			"field2": 0,
			"field3": storage.SizeOfInt,
			"field4": 0,
		},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)
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
			recordLength += storage.Offset(storage.UnsafeSizeOfStringAsVarlen(val))
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
		storage.UnsafeIntegerToFixedlen(layout.sizes["field1"], record[0].(storage.TinyInt)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field2",
		storage.UnsafeNewVarlenFromGoString(record[1].(string)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetFixedLen(
		slot,
		"field3",
		storage.UnsafeIntegerToFixedlen(layout.sizes["field3"], record[2].(storage.Int)),
	); err != nil {
		t.Fatal(err)
	}

	if err := page.SetVarLen(
		slot,
		"field4",
		storage.UnsafeNewVarlenFromGoString(record[3].(string)),
	); err != nil {
		t.Fatal(err)
	}

	gotFixed, err := page.FixedLen(slot, "field1")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeFixedToInteger[storage.TinyInt](gotFixed); v != record[0].(storage.TinyInt) {
		t.Errorf("expected field1 value to be %d, got %d", record[0], v)
	}

	gotVarlen, err := page.VarLen(slot, "field2")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeVarlenToGoString(gotVarlen); v != record[1].(string) {
		t.Errorf("expected field2 value to be %s, got %s", record[1], v)
	}

	gotFixed, err = page.FixedLen(slot, "field3")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeFixedToInteger[storage.Int](gotFixed); v != record[2].(storage.Int) {
		t.Errorf("expected field3 value to be %d, got %d", record[2], v)
	}

	gotVarlen, err = page.VarLen(slot, "field4")
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeVarlenToGoString(gotVarlen); v != record[3].(string) {
		t.Errorf("expected field4 value to be %s, got %s", record[3], v)
	}
}

func TestSlottedPageSetAtSpecial(t *testing.T) {

	page := NewSlottedPage(newMockTx(), storage.NewBlock("file", 1), nil)

	var specialSpaceSize storage.Offset = 512

	if err := page.Format(specialSpaceSize); err != nil {
		t.Fatalf("error formatting page: %v", err)
	}

	t.Run("format header includes special space size", func(t *testing.T) {

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
		const exp storage.Int = 12345
		f := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, exp)

		if err := page.SetFixedLenAtSpecial(0, storage.SizeOfInt, f); err != nil {
			t.Fatalf("error setting fixedlen at special slot: %v", err)
		}

		got, err := page.FixedLenAtSpecial(0, storage.SizeOfInt)
		if err != nil {
			t.Fatalf("error getting fixedlen at special slot: %v", err)
		}

		if v := storage.UnsafeFixedToInteger[storage.Int](got); v != exp {
			t.Errorf("expected value to be %d, got %d", exp, v)
		}
	})

	t.Run("set varlen at special slot", func(t *testing.T) {
		const exp = "This is a string"
		v := storage.UnsafeNewVarlenFromGoString(exp)

		if err := page.SetVarLenAtSpecial(0, v); err != nil {
			t.Fatalf("error setting varlen at special slot: %v", err)
		}

		got, err := page.VarLenAtSpecial(0)
		if err != nil {
			t.Fatalf("error getting varlen at special slot: %v", err)
		}

		if v := storage.UnsafeVarlenToGoString(got); v != exp {
			t.Errorf("expected value to be %s, got %s", exp, v)
		}
	})

}

func TestSlottedPageDelete(t *testing.T) {
	tx := newMockTx()
	layout := mockLayout{
		indexes: map[string]int{"field1": 0},
	}

	page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

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

func TestSlottedPageCompact(t *testing.T) {
	t.Run("compact page", func(t *testing.T) {
		tx := newMockTx()
		layout := mockLayout{
			indexes: map[string]int{"field1": 0},
		}

		page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

		if err := page.Format(0); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		recordHeaderSize := page.recordHeaderSize()

		const (
			firstValue  storage.Int = 255
			secondValue storage.Int = 1023
			thirdValue  storage.Int = 999813
		)
		for i, v := range []storage.Int{firstValue, secondValue, thirdValue} {

			slot, err := page.InsertAfter(BeforeFirstSlot, storage.Offset(storage.SizeOfInt), false)
			if err != nil {
				t.Fatalf("error inserting record: %v", err)
			}

			if slot != storage.SmallInt(i) {
				t.Fatalf("expected slot to be %d, got %d", i, slot)
			}

			f := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, v)

			if err := page.SetFixedLen(slot, "field1", f); err != nil {
				t.Fatalf("error setting fixedlen: %v", err)
			}
		}

		recordSize := storage.Offset(storage.SizeOfInt) + recordHeaderSize

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

			if got := storage.UnsafeFixedToInteger[storage.Int](f); got != want {
				t.Fatalf("expected value at slot %d to be %d, got %d", slot, want, got)
			}
		}
	})

	t.Run("fill page with records and compact", func(t *testing.T) {
		tx := newMockTx()
		layout := mockLayout{
			indexes: map[string]int{"field1": 0},
		}

		page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

		if err := page.Format(0); err != nil {
			t.Fatalf("error formatting page: %v", err)
		}

		for {
			slot, err := page.InsertAfter(BeforeFirstSlot, storage.Offset(storage.SizeOfInt), false)
			if err == ErrNoFreeSlot {

				break
			}

			if err != nil {
				t.Fatalf("error inserting record: %v", err)
			}

			f := storage.UnsafeIntegerToFixedlen[storage.Long](storage.SizeOfInt, storage.Long(slot))
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

func BenchmarkSlottedPageCompact(b *testing.B) {

	preparePage := func() *SlottedPage {
		tx := newMockTx()
		layout := mockLayout{
			indexes: map[string]int{"field1": 0},
		}

		page := NewSlottedPage(tx, storage.NewBlock("file", 1), layout)

		if err := page.Format(0); err != nil {
			b.Fatalf("error formatting page: %v", err)
		}

		for {
			slot, err := page.InsertAfter(BeforeFirstSlot, storage.Offset(storage.SizeOfInt), false)
			if err == ErrNoFreeSlot {

				break
			}

			if err != nil {
				b.Fatalf("error inserting record: %v", err)
			}

			f := storage.UnsafeIntegerToFixedlen[storage.Long](storage.SizeOfInt, storage.Long(slot))
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

	p := preparePage()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := p.Compact(); err != nil {
			b.Fatalf("error compacting page: %v", err)
		}

		b.StopTimer()
		p.Close()
		p = preparePage()
		b.StartTimer()
	}

}
