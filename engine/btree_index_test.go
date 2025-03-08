package engine

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestCreateNewBTreeIndex(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	leafSchema := newSchema()
	leafSchema.addField(indexFieldDataVal, storage.INT)
	leafSchema.addField(indexFieldBlockNumber, storage.LONG)
	leafSchema.addField(indexFieldRecordID, storage.INT)

	t.Run("Create new BTree index", func(t *testing.T) {
		leafLayout := NewLayout(leafSchema)

		idxName := test.RandomName()
		expectedLeafTable := idxName + "_leaf"
		expectedDirTable := idxName + "_dir"

		_, err := NewBTreeIndex(x, idxName, leafLayout)
		if err != nil {
			t.Fatalf("Error creating new BTree index: %v", err)
		}

		size, err := x.Size(expectedLeafTable)
		if err != nil {
			t.Fatalf("Error getting size of leaf table: %v", err)
		}

		if size == 0 {
			t.Fatalf("Expected leaf table to have size > 0, got %d", size)
		}

		size, err = x.Size(expectedDirTable)
		if err != nil {
			t.Fatalf("Error getting size of dir table: %v", err)
		}

		if size == 0 {
			t.Fatalf("Expected dir table to have size > 0, got %d", size)
		}
	})
}

func TestBTreeIndexInsertFixedLen(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	leafSchema := newSchema()
	leafSchema.addField(indexFieldDataVal, storage.LONG)
	leafSchema.addField(indexFieldBlockNumber, storage.LONG)
	leafSchema.addField(indexFieldRecordID, storage.INT)

	leafLayout := NewLayout(leafSchema)

	index, err := NewBTreeIndex(x, test.RandomName(), leafLayout)
	if err != nil {
		t.Fatalf("Error creating new BTree index: %v", err)
	}

	t.Run("inserts and finds a record in BTree index", func(t *testing.T) {
		v := storage.Long(123)
		val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, v)

		rid := NewRID(123, storage.SmallInt(v))

		if err := index.Insert(val, rid); err != nil {
			t.Fatalf("Error inserting record into BTree index: %v", err)
		}

		if err := index.BeforeFirst(val); err != nil {
			t.Fatalf("Error before first in BTree index: %v", err)
		}

		if err := index.Next(); err != nil {
			t.Fatalf("Error next in BTree index: %v", err)
		}

		rid, err = index.DataRID()
		if err != nil {
			t.Fatalf("Error getting data RID in BTree index: %v", err)
		}

		if rid.Blocknum != 123 {
			t.Fatalf("Expected block number to be 123, got %d", rid.Blocknum)
		}

		if rid.Slot != storage.SmallInt(v) {
			t.Fatalf("Expected record ID to be %d, got %d", v, rid.Slot)
		}
	})

	t.Run("Inserts and finds records in BTree index", func(t *testing.T) {
		inserted := make([]storage.Long, 0, 1000)

		for i := range 1000 {
			n := rand.Intn(math.MaxUint16)
			v := storage.Long(uint16(n))

			val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, v)

			rid := NewRID(123, storage.SmallInt(v))

			err := index.Insert(val, rid)
			if err != nil {
				t.Fatalf("Error inserting record into BTree index at iteration %d: %v", i, err)
			}

			inserted = append(inserted, v)
		}

		root := newBTreeDir(x, index.rootBlock, index.dirLayout)
		records, err := root.contents.numRecords()
		if err != nil {
			t.Fatalf("Error getting number of records in BTree index: %v", err)
		}

		for i := range records {
			key, err := root.contents.dataVal(i)
			if err != nil {
				t.Fatalf("Error getting data value in BTree index at iteration %d: %v", i, err)
			}

			v := key.AsFixedLen().AsLong()

			blocknum, err := root.contents.getBlockNumber(i)
			if err != nil {
				t.Fatalf("Error getting block number in BTree index at iteration %d: %v", i, err)
			}

			block := storage.NewBlock(index.leafTable, blocknum)
			node, err := newBTreeLeaf(x, block, index.leafLayout, storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 0))
			if err != nil {
				t.Fatalf("Error creating new BTree leaf in BTree index at iteration %d: %v", i, err)
			}

			dump, err := node.contents.dump()
			if err != nil {
				t.Fatalf("Error dumping node")
			}

			fv := dump.Datavals[0].AsFixedLen().AsLong()

			typ := dump.ValType

			if i != 0 && fv != v {
				t.Logf(
					"First entry in the BTree leaf does not equal the directory. Expected %d, got %d",
					v,
					fv,
				)
			}

			for i := range dump.Datavals {
				if i > 0 {
					prev := dump.Datavals[i-1]
					curr := dump.Datavals[i]

					if curr.Less(typ, prev) {
						t.Fatalf("Expected %v to be less than %v", prev, curr)
					}
				}
			}
		}

		for i, v := range inserted {
			val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, v)

			err := index.BeforeFirst(val)
			if err != nil {
				t.Fatalf("Error before first in BTree index at iteration %d: %v", i, err)
			}

			if err := index.Next(); err != nil {
				t.Fatalf("Error next in BTree index at iteration %d: %v", i, err)
			}

			rid, err := index.DataRID()
			if err != nil {
				t.Fatalf("Error getting data RID in BTree index at iteration %d: %v", i, err)
			}

			if rid.Blocknum != 123 {
				t.Fatalf("Expected block number to be 123, got %d", rid.Blocknum)
			}

			if rid.Slot != storage.SmallInt(v) {
				t.Fatalf("Expected record ID to be %d, got %d", v, rid.Slot)
			}
		}
	})
}

func TestBTreeIndexInsertVarlen(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	leafSchema := newSchema()
	leafSchema.addField(indexFieldDataVal, storage.TEXT)
	leafSchema.addField(indexFieldBlockNumber, storage.LONG)
	leafSchema.addField(indexFieldRecordID, storage.INT)

	leafLayout := NewLayout(leafSchema)

	index, err := NewBTreeIndex(x, test.RandomName(), leafLayout)
	if err != nil {
		t.Fatalf("Error creating new BTree index: %v", err)
	}

	t.Run("inserts and finds varlen records in BTree index", func(t *testing.T) {
		inserted := make([]string, 0, 1000)

		for i := range 1000 {
			n := rand.Intn(math.MaxUint16)
			padding := rand.Intn(16)

			str := fmt.Sprintf("%s (record %d)", test.RandomStringOfSize(padding), n)

			val := storage.ValueFromGoString(str)

			rid := NewRID(123, storage.SmallInt(n))

			err := index.Insert(val, rid)
			if err != nil {
				t.Fatalf("Error inserting record into BTree index at iteration %d: %v", i, err)
			}

			inserted = append(inserted, str)
		}

		root := newBTreeDir(x, index.rootBlock, index.dirLayout)
		records, err := root.contents.numRecords()
		if err != nil {
			t.Fatalf("Error getting number of records in BTree index: %v", err)
		}

		for i := range records {
			key, err := root.contents.dataVal(i)
			if err != nil {
				t.Fatalf("Error getting data value in BTree index at iteration %d: %v", i, err)
			}

			v := key.AsVarlen().AsGoString()

			blocknum, err := root.contents.getBlockNumber(i)
			if err != nil {
				t.Fatalf("Error getting block number in BTree index at iteration %d: %v", i, err)
			}

			block := storage.NewBlock(index.leafTable, blocknum)
			node, err := newBTreeLeaf(x, block, index.leafLayout, storage.ValueFromInteger[storage.Long](storage.SizeOfLong, 0))
			if err != nil {
				t.Fatalf("Error creating new BTree leaf in BTree index at iteration %d: %v", i, err)
			}

			dump, err := node.contents.dump()
			if err != nil {
				t.Fatalf("Error dumping node")
			}

			fv := dump.Datavals[0].AsVarlen().AsGoString()

			typ := dump.ValType

			if i != 0 && fv != v {
				t.Logf(
					"First entry in the BTree leaf does not equal the directory. Expected %q, got %q",
					v,
					fv,
				)
			}

			for i := range dump.Datavals {
				if i > 0 {
					prev := dump.Datavals[i-1]
					curr := dump.Datavals[i]

					if curr.Less(typ, prev) {
						t.Fatalf("Expected %v to be less than %v", prev, curr)
					}
				}
			}
		}

		for i, v := range inserted {
			val := storage.ValueFromGoString(v)

			err := index.BeforeFirst(val)
			if err != nil {
				t.Fatalf("Error before first in BTree index at iteration %d: %v", i, err)
			}

			if err := index.Next(); err != nil {
				t.Fatalf("Error next in BTree index at iteration %d: %v", i, err)
			}

			rid, err := index.DataRID()
			if err != nil {
				t.Fatalf("Error getting data RID in BTree index at iteration %d: %v", i, err)
			}

			if rid.Blocknum != 123 {
				t.Fatalf("Expected block number to be 123, got %d", rid.Blocknum)
			}
		}
	})
}

func TestBTreeIndexDelete(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	leafSchema := newSchema()
	leafSchema.addField(indexFieldDataVal, storage.LONG)
	leafSchema.addField(indexFieldBlockNumber, storage.LONG)
	leafSchema.addField(indexFieldRecordID, storage.INT)

	leafLayout := NewLayout(leafSchema)

	index, err := NewBTreeIndex(x, test.RandomName(), leafLayout)
	if err != nil {
		t.Fatalf("Error creating new BTree index: %v", err)
	}

	t.Run("deletes a record in BTree index", func(t *testing.T) {
		v := storage.Long(123)
		val := storage.ValueFromInteger[storage.Long](storage.SizeOfLong, v)

		rid := NewRID(123, storage.SmallInt(v))

		if err := index.Insert(val, rid); err != nil {
			t.Fatalf("Error inserting record into BTree index: %v", err)
		}

		if err := index.BeforeFirst(val); err != nil {
			t.Fatalf("Error before first in BTree index: %v", err)
		}

		if err := index.Next(); err != nil {
			t.Fatalf("Error next in BTree index: %v", err)
		}

		rid, err = index.DataRID()
		if err != nil {
			t.Fatalf("Error getting data RID in BTree index: %v", err)
		}

		if rid.Blocknum != 123 {
			t.Fatalf("Expected block number to be 123, got %d", rid.Blocknum)
		}

		if rid.Slot != storage.SmallInt(v) {
			t.Fatalf("Expected record ID to be %d, got %d", v, rid.Slot)
		}

		if err := index.Delete(val, rid); err != nil {
			t.Fatalf("Error deleting record from BTree index: %v", err)
		}

		if err := index.BeforeFirst(val); err != nil {
			t.Fatalf("Error before first in BTree index: %v", err)
		}

		if err := index.Next(); err != io.EOF {
			t.Fatalf("Expected EOF, got %v", err)
		}
	})
}