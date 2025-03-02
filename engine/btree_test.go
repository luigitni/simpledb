package engine

import (
	"fmt"
	"io"
	"math"
	"slices"
	"testing"

	"math/rand"

	"github.com/luigitni/simpledb/pages"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestBTreePageSplit(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	schema := newSchema()
	schema.addField("field", storage.SMALLINT)
	layout := NewLayout(schema)

	block := storage.NewBlock("testfile", 0)
	x.Append(block.FileName())

	page := newBTreePage(x, block, layout)
	if err := page.format(flagUnset); err != nil {
		t.Fatalf("unexpected error when formatting the page: %s", err)
	}

	for i := range 100 {
		slot := storage.SmallInt(i)

		if err := page.slottedPage.InsertAt(slot, storage.Offset(storage.SizeOfSmallInt)); err != nil {
			t.Fatalf("unexpected error when inserting record: %s", err)
		}

		f := storage.UnsafeIntegerToFixedlen(storage.SizeOfSmallInt, storage.SmallInt(i))

		if err := page.slottedPage.SetFixedLen(slot, "field", f); err != nil {
			t.Fatalf("unexpected error when setting field: %s", err)
		}

		page.setNumRecords(slot + 1)
	}

	var splitpos storage.SmallInt = 25

	newBlock, err := page.split(splitpos, flagUnset)
	if err != nil {
		t.Fatalf("unexpected error when splitting the page: %s", err)
	}

	// test that the new block has the correct number of records
	splitted := newBTreePage(x, newBlock, layout)

	numRecordsNewPage, err := splitted.numRecords()
	if err != nil {
		t.Fatalf("unexpected error when getting number of records: %s", err)
	}

	if numRecordsNewPage != 75 {
		t.Fatalf("expected 75 records, got %d", numRecordsNewPage)
	}

	// test that the old block has the correct number of records
	numRecordsOldPage, err := page.numRecords()
	if err != nil {
		t.Fatalf("unexpected error when getting number of records: %s", err)
	}

	if numRecordsOldPage != 25 {
		t.Fatalf("expected 25 records, got %d", numRecordsOldPage)
	}

	// test that the records are in the correct position
	test := func(page bTreePage, i, exp storage.SmallInt) error {
		v, err := page.slottedPage.FixedLen(i, "field")
		if err != nil {

			return fmt.Errorf("unexpected error when getting field: %s", err)
		}

		if got := storage.UnsafeFixedToInteger[storage.SmallInt](v); got != exp {

			return fmt.Errorf("expected %d at %d, got %d", exp, i, got)
		}

		return nil
	}

	// test that before the pivot, the records are in the correct position
	for i := 0; i < int(numRecordsOldPage); i++ {
		if err := test(page, storage.SmallInt(i), storage.SmallInt(i)); err != nil {
			t.Fatal(err)
		}
	}

	// test that after the pivot, the records are in the correct position
	for i := 0; i < int(numRecordsNewPage); i++ {
		if err := test(splitted, storage.SmallInt(i), storage.SmallInt(i+25)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBTreeFindSlotBefore(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	schema := newSchema()
	schema.addField(indexFieldDataVal, storage.LONG)
	layout := NewLayout(schema)

	block := storage.NewBlock("testfile", 0)
	x.Append(block.FileName())

	page := newBTreePage(x, block, layout)
	if err := page.format(flagUnset); err != nil {
		t.Fatalf("unexpected error when formatting the page: %s", err)
	}

	size := storage.Offset(layout.FieldSize(indexFieldDataVal))
	for i := range 50 {
		val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, storage.Long(i))
		slot := storage.SmallInt(i)

		if _, err := page.insert(slot, val, size); err != nil {
			t.Fatalf("unexpected error when inserting record: %s", err)
		}
	}

	for _, tc := range []struct {
		val  storage.Value
		slot storage.SmallInt
	}{
		{
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 0),
			pages.BeforeFirstSlot,
		},
		{
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 1),
			0,
		},
		{
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 31),
			30,
		},
		{
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 50),
			49,
		},
		{
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 123),
			49,
		},
	} {

		slot, err := page.findSlotBefore(tc.val)
		if err != nil {
			t.Fatalf("unexpected error when finding slot before: %s", err)
		}

		if slot != tc.slot {
			t.Fatalf("expected slot %d, got %d", tc.slot, slot)
		}
	}
}

func TestBTreeLeafInsertLeafRecord(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	schema := newSchema()
	// assume the indexed property is a TEXT
	schema.addField(indexFieldDataVal, storage.TEXT)
	schema.addField(indexFieldBlockNumber, storage.LONG)
	schema.addField(indexFieldRecordID, storage.SMALLINT)

	layout := NewLayout(schema)

	t.Run("test insert without splitting", func(t *testing.T) {
		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock("leaf", 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromGoString("start"),
		)

		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		if err := leaf.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		var slot storage.SmallInt
		for slot = 0; slot < 100; slot++ {
			s := fmt.Sprintf("record %d", slot)

			leaf.key = storage.ValueFromGoString(s)

			rid := NewRID(block.Number(), storage.SmallInt(slot))
			if err := leaf.contents.insertLeafRecord(slot, leaf.key, rid); err != nil {
				t.Fatalf("unexpected error when inserting leaf record: %s", err)
			}
		}

		// test that the records are in the correct position
		for i := range 100 {
			slot := storage.SmallInt(i)
			rid, err := leaf.contents.dataRID(slot)
			if err != nil {
				t.Fatalf("unexpected error when getting leaf record: %s", err)
			}

			if got := rid.Slot; got != slot {
				t.Fatalf("expected slot %d, got %d", slot, got)
			}
		}
	})
}

func TestBTreeLeafInsert(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	t.Run("insert out of order records", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.INT)
		schema.addField(indexFieldBlockNumber, storage.LONG)
		schema.addField(indexFieldRecordID, storage.SMALLINT)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 0),
		)

		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		if err := leaf.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		recs := []storage.Int{
			18,
			1,
			3,
			456,
			9999823,
			0,
			18,
		}

		for i, rec := range recs {

			key := storage.ValueFromInteger[storage.Int](storage.SizeOfInt, rec)

			flag, err := leaf.contents.flag()
			if err != nil {
				t.Fatalf("unexpected error when getting the flag: %s", err)
			}

			if flag != flagUnset {
				t.Fatalf("expected flag %d, got %d at iteration %d", flagUnset, flag, i)
			}

			leaf.key = key
			leaf.currentSlot, err = leaf.contents.findSlotBefore(leaf.key)
			if err != nil {
				t.Fatalf("unexpected error when finding slot before: %s", err)
			}

			rid := NewRID(block.Number(), storage.SmallInt(i))

			if _, err := leaf.insert(rid); err != nil {
				t.Fatalf("unexpected error when inserting record %d: %s", rec, err)
			}
		}

		cpy := make([]storage.Int, len(recs))
		copy(cpy, recs)

		slices.SortFunc(cpy, func(i, j storage.Int) int {
			return int(i) - int(j)
		})

		// test that records are in order
		for i, rec := range cpy {
			val, err := leaf.contents.dataVal(storage.SmallInt(i))
			if err != nil {
				t.Fatalf("unexpected error when getting data value: %s", err)
			}

			if got := storage.ValueAsInteger[storage.Int](val); got != rec {
				t.Fatalf("expected %d, got %d", cpy[i], got)
			}
		}

	})

	t.Run("insert a record in a leaf page without overflow", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.INT)
		schema.addField(indexFieldBlockNumber, storage.LONG)
		schema.addField(indexFieldRecordID, storage.SMALLINT)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 0),
		)
		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		if err := leaf.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		for i := 0; i < 100; i++ {
			i := storage.Int(i)

			leaf.key = storage.ValueFromInteger[storage.Int](storage.SizeOfInt, i)

			rid := NewRID(block.Number(), storage.SmallInt(i))

			leaf.currentSlot, err = leaf.contents.findSlotBefore(leaf.key)
			if err != nil {
				t.Fatalf("unexpected error when finding slot before: %s", err)
			}

			dir, err := leaf.insert(rid)
			if err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}

			if !dir.empty() {
				t.Fatalf("expected empty dir when inserting %dth value, got %v", i, dir)
			}
		}
	})

	t.Run("insert a smaller record in a leaf page with overflow causes a split", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.INT)
		schema.addField(indexFieldBlockNumber, storage.LONG)
		schema.addField(indexFieldRecordID, storage.SMALLINT)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 0),
		)

		// format the leaf page with an overflow block
		if err := leaf.contents.format(123); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		// insert increasing records, don't expect a split
		for i := 1000; i < 1010; i++ {
			i := storage.Int(i)

			leaf.key = storage.ValueFromInteger[storage.Int](storage.SizeOfInt, i)

			leaf.currentSlot, err = leaf.contents.findSlotBefore(leaf.key)
			if err != nil {
				t.Fatalf("unexpected error when finding slot before: %s", err)
			}

			rid := NewRID(block.Number(), storage.SmallInt(i))
			dir, err := leaf.insert(rid)
			if err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}

			if !dir.empty() {
				t.Fatalf("expected empty dir when inserting %dth value, got %v", i-1000, dir)
			}
		}

		// insert a record that causes a split
		rid := NewRID(block.Number(), 5)
		leaf.key = storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 5)

		dir, err := leaf.insert(rid)
		if err != nil {
			t.Fatalf("unexpected error when inserting record: %s", err)
		}

		if dir.empty() {
			t.Fatalf("expected non-empty dir, got %v", dir)
		}
	})

	t.Run("insert in a full leaf page causes a split", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.LONG)
		schema.addField(indexFieldBlockNumber, storage.LONG)
		schema.addField(indexFieldRecordID, storage.SMALLINT)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 0),
		)

		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		if err := leaf.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		var i storage.SmallInt
		for {
			leaf.key = storage.ValueFromInteger[storage.Long](storage.SizeOfLong, storage.Long(i))

			fits, err := leaf.contents.slottedPage.RecordsFit(
				storage.Offset(storage.SizeOfLong+storage.SizeOfSmallInt+storage.SizeOfLong),
				bTreeMaxSizeOfKey,
			)

			shouldSplit := !fits

			if err != nil {
				t.Fatalf("unexpected error when checking if record fits: %s", err)
			}

			leaf.currentSlot, err = leaf.contents.findSlotBefore(leaf.key)
			if err != nil {
				t.Fatalf("unexpected error when finding slot before: %s", err)
			}

			rid := NewRID(block.Number(), i)
			dir, err := leaf.insert(rid)
			if err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}

			if !shouldSplit && !dir.empty() {
				t.Fatalf("expected empty dir when inserting %dth value, got %v.", i, dir)
			}

			if shouldSplit {
				if dir.empty() {
					t.Fatalf("expected non-empty dir when inserting %dth value, got %v", i, dir)
				}

				break
			}

			i++
		}
	})
}

func TestBTreeLeafNext(t *testing.T) {
	t.Run("finds the key", func(t *testing.T) {
		fm, lm, bm := test.MakeManagers(t)

		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.INT)
		schema.addField(indexFieldBlockNumber, storage.LONG)
		schema.addField(indexFieldRecordID, storage.SMALLINT)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		leaf, err := newBTreeLeaf(
			x,
			block,
			layout,
			storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 0),
		)

		if err != nil {
			t.Fatalf("unexpected error when creating the leaf page: %s", err)
		}

		if err := leaf.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the leaf page: %s", err)
		}

		values := []storage.Int{18, 1, 3, 456, 9999823, 0, 18}

		for i, v := range values {
			id := storage.SmallInt(i)
			leaf.key = storage.ValueFromInteger[storage.Int](storage.SizeOfInt, v)

			rid := NewRID(block.Number(), id)

			leaf.currentSlot, err = leaf.contents.findSlotBefore(leaf.key)
			if err != nil {
				t.Fatalf("unexpected error when finding slot before: %s", err)
			}

			if _, err := leaf.insert(rid); err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}
		}

		for _, v := range values {
			leaf.currentSlot = pages.BeforeFirstSlot
			key := storage.ValueFromInteger[storage.Int](storage.SizeOfInt, v)

			leaf.key = key

			for {
				found, err := leaf.next()
				if err == io.EOF {
					break
				}

				if err != nil {
					t.Fatalf("unexpected error when finding next: %s", err)
				}

				if found {
					got, err := leaf.contents.dataVal(leaf.currentSlot)
					if err != nil {
						t.Fatalf("unexpected error when getting data value: %s", err)
					}

					if !got.Equals(key) {
						t.Fatalf("expected key %d, got %d", key, got)
					}

					break
				}
			}
		}
	})
}

func TestBTreeDirInsertEntry(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	t.Run("insert directory entries does not split if space is avaliable", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.LONG)
		schema.addField(indexFieldBlockNumber, storage.LONG)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		page := newBTreeDir(
			x,
			block,
			layout,
		)

		if err := page.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the directory page: %s", err)
		}

		for i := 0; i < 100; i++ {
			i := storage.Long(i)

			dir := dirEntry{
				blockNum: i,
				value:    storage.ValueFromInteger[storage.Long](storage.SizeOfLong, i),
			}

			out, err := page.insertEntry(dir)
			if err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}

			if !out.empty() {
				t.Fatalf("expected empty dir when inserting %dth value, got %v", i, dir)
			}
		}
	})

	t.Run("insert directory entries splits if space is not avaliable", func(t *testing.T) {
		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.LONG)
		schema.addField(indexFieldBlockNumber, storage.LONG)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		page := newBTreeDir(
			x,
			block,
			layout,
		)

		if err := page.contents.format(flagUnset); err != nil {
			t.Fatalf("unexpected error when formatting the directory page: %s", err)
		}

		var i int
		splitAt := -1
		for {

			fits, err := page.contents.slottedPage.RecordsFit(
				storage.Offset(storage.SizeOfLong+storage.SizeOfLong),
				bTreeMaxSizeOfKey,
			)

			if err != nil {
				t.Fatalf("unexpected error when checking if record fits: %s", err)
			}

			if !fits && splitAt == -1 {
				splitAt = i + 1
			}

			val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, storage.Long(i))

			dir := dirEntry{
				blockNum: storage.Long(i),
				value:    val,
			}

			out, err := page.insertEntry(dir)
			if err != nil {
				t.Fatalf("unexpected error when inserting record: %s", err)
			}

			if i != splitAt && !out.empty() {
				t.Fatalf("expected empty dir when inserting %dth value, got %v.", i, out)
			}

			if i == splitAt {
				if out.empty() {
					t.Fatalf("expected non-empty dir when inserting %dth value, got %v", i, out)
				}

				break
			}

			if !out.empty() {
				break
			}

			i++
		}
	})
}

func TestBTreeDirInsert(t *testing.T) {
	t.Run("insert records into directory pages", func(t *testing.T) {
		fm, lm, bm := test.MakeManagers(t)

		schema := newSchema()
		schema.addField(indexFieldDataVal, storage.LONG)
		schema.addField(indexFieldBlockNumber, storage.LONG)

		layout := NewLayout(schema)

		x := tx.NewTx(fm, lm, bm)
		defer x.Commit()

		block := storage.NewBlock(test.RandomName(), 0)
		x.Append(block.FileName())

		root := newBTreeDir(
			x,
			block,
			layout,
		)

		if err := root.contents.format(flagBTreeRoot); err != nil {
			t.Fatalf("unexpected error when formatting the directory page: %s", err)
		}

		for i := 0; i < 10000; i++ {

			n := rand.Intn(math.MaxInt)

			val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, storage.Long(n))

			dir := dirEntry{
				blockNum: block.Number(),
				value:    val,
			}

			_, err := root.insert(dir)
			if err != nil {
				t.Fatalf("unexpected error when inserting record at iteration %d: %s", i, err)
			}
		}
	})
}

func BenchmarkDirectoryPageSplit(b *testing.B) {

	b.StopTimer()
	fm, lm, bm := test.MakeManagersWithDir(b.TempDir())

	schema := newSchema()
	schema.addField(indexFieldDataVal, storage.LONG)
	schema.addField(indexFieldBlockNumber, storage.LONG)

	layout := NewLayout(schema)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	block := storage.NewBlock(test.RandomName(), 0)
	x.Append(block.FileName())

	page := newBTreeDir(
		x,
		block,
		layout,
	)

	if err := page.contents.format(flagBTreeRoot); err != nil {
		b.Fatalf("unexpected error when formatting the directory page: %s", err)
	}

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, storage.Long(i))

		dir := dirEntry{
			blockNum: storage.Long(i),
			value:    val,
		}

		out, err := page.insert(dir)
		if err != nil {
			b.Fatalf("unexpected error when inserting record: %s", err)
		}

		if !out.empty() {
			b.ResetTimer()
		}
	}
}
