package record

import (
	"testing"

	"github.com/luigitni/simpledb/file"
)

func TestAppenHeaderEntry(t *testing.T) {
	tx := newMockTx()
	page, err := newStringPage(tx, file.Block{})
	
	if err != nil {
		t.Fatal(err)
	}

	entry := headerEntry{
		offset:    10,
		nextChunk: RID{Blocknum: 0, Slot: 1},
	}

	if err := page.appendHeaderEntry(entry); err != nil {
		t.Fatal(err)
	}

	if got, want := len(page.header.entries), 1; got != want {
		t.Errorf("got %d entries, want %d", got, want)
	}

	if page.header.entries[0] != entry {
		t.Errorf("appended entry mismatch, got %v want %v", page.header.entries[0], entry)
	}
}

func TestReadHeader(t *testing.T) {
	tx := newMockTx()

	const (
		freeSpaceEnd = 1024
	)

	entries := []headerEntry{
		{
			offset:    0,
			nextChunk: RID{23, 12},
		},
		{
			offset:    256,
			nextChunk: terminationRID,
		},
		{
			offset:    512,
			nextChunk: RID{41, 3},
		},
	}

	records := len(entries)

	page, err := newStringPage(tx, file.Block{})
	if err != nil {
		t.Fatal(err)
	}

	tx.SetInt(page.block, numRecordsOffset, records, false)
	tx.SetInt(page.block, freeSpaceEndOffset, freeSpaceEnd, false)

	for _, e := range entries {
		if err := page.appendHeaderEntry(e); err != nil {
			t.Fatal(err)
		}
	}

	header, err := page.readHeader()
	if err != nil {
		t.Fatal(err)
	}

	if header.numRecords != records {
		t.Errorf("got numRecords %d, want %d", header.numRecords, records)
	}

	if header.freeSpaceEnd != freeSpaceEnd {
		t.Errorf("got freeSpaceEnd %d, want %d", header.freeSpaceEnd, freeSpaceEnd)
	}

	if header.numRecords != len(entries) {
		t.Errorf("got numRecords %d, want %d", header.numRecords, len(entries))
	}

	for i, e := range entries {
		if header.entries[i] != e {
			t.Errorf("entry mismatch at index %d, got %v want %v", i, header.entries[i], e)
		}
	}
}