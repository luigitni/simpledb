package log

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
)

func TestAppend(t *testing.T) {
	dbFolder := t.TempDir()
	logfile := "wal_test"
	blockSize := storage.PageSize

	fman := file.NewFileManager(dbFolder, storage.Long(blockSize))
	lm := NewWalWriter(fman, logfile)

	t.Run("increments the latestLSN", func(t *testing.T) {
		for i := range 10 {
			lm.Append([]byte(fmt.Sprintf("record_%d", i)))
		}

		if lm.latestLSN != 10 {
			t.Fatalf("expected 10, got %d", lm.latestLSN)
		}
	})

	t.Run("returns the correct LSN", func(t *testing.T) {
		lsn := lm.Append([]byte("record"))
		if lsn != 11 {
			t.Fatalf("expected 11, got %d", lsn)
		}
	})

	t.Run("returns the correct LSN after a flush", func(t *testing.T) {
		lm.Flush(11)
		lsn := lm.Append([]byte("record"))
		if lsn != 12 {
			t.Fatalf("expected 12, got %d", lsn)
		}
	})

	t.Run("adds a block if the current one is full", func(t *testing.T) {
		if lm.currentBlock.Number() != 0 {
			t.Fatalf("expected 0, got %d", lm.currentBlock.Number())
		}

		for i := 0; i <= storage.PageSize+1024; {
			record := []byte(fmt.Sprintf("record_%d", i))
			lm.Append(record)

			i += len(record) + int(storage.SizeOfOffset)
		}

		if lm.currentBlock.Number() != 1 {
			t.Fatalf("expected 2, got %d", lm.currentBlock.Number())
		}
	})
}

func TestIterator(t *testing.T) {
	dbFolder := t.TempDir()
	logfile := "wal_test"
	blockSize := storage.PageSize

	fman := file.NewFileManager(dbFolder, storage.Long(blockSize))
	lm := NewWalWriter(fman, logfile)

	t.Run("returns an empty iterator if the log is empty", func(t *testing.T) {
		iter := lm.Iterator()

		if iter.Next() != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("returns one record", func(t *testing.T) {
		record := []byte("record")

		lm.Append(record)

		iter := lm.Iterator()
		got := iter.Next()

		if !bytes.Equal(got, record) {
			t.Fatalf("expected %s, got %s", string(record), string(got))
		}
	})

	t.Run("returns the correct records", func(t *testing.T) {
		populateLogManager(t, lm, 1, 10)
		iter := lm.Iterator()

		for i := 10; i > 0; i-- {
			record := iter.Next()

			if !bytes.Equal(record, []byte(makeLogEntry(t, i))) {
				t.Fatalf("expected %s, got %s", fmt.Sprintf("record_%d", i), string(record))
			}
		}
	})

	t.Run("returns the correct records after a flush", func(t *testing.T) {
		populateLogManager(t, lm, 11, 20)
		lm.Flush(15)

		iter := lm.Iterator()

		for i := 20; i > 10; i-- {
			record := iter.Next()

			if !bytes.Equal(record, []byte(makeLogEntry(t, i))) {
				t.Fatalf("expected %s, got %s", fmt.Sprintf("record_%d", i), string(record))
			}
		}
	})
}

func makeLogEntry(t *testing.T, idx int) string {
	t.Helper()
	return fmt.Sprintf("record_%d", idx)
}

// testLogIteration verifies that logs are returned in a LIFO manner
// populateLogManager appends logs of format K -> V to the logfile
func populateLogManager(t *testing.T, lm *WalWriter, start, end int) {
	t.Helper()

	t.Log("Creating log records:")

	var builder bytes.Buffer
	for i := start; i <= end; i++ {

		builder.Reset()
		builder.WriteString(makeLogEntry(t, i))

		lsn := lm.Append(builder.Bytes())
		t.Logf("%d", lsn)
	}
	t.Log("Records created.")
}
