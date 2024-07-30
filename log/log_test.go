package log_test

import (
	"fmt"
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
	"github.com/luigitni/simpledb/test"
)

func TestLog(t *testing.T) {

	conf := test.DefaultConfig(t)
	dbFolder := conf.DbFolder
	logfile := conf.LogFile
	blockSize := conf.BlockSize

	fman := file.NewFileManager(dbFolder, blockSize)

	lm := log.NewLogManager(fman, logfile)

	populateLogManager(t, lm, 1, 35)
	testLogIteration(t, lm, 35)
	populateLogManager(t, lm, 36, 70)

	lm.Flush(65)
	testLogIteration(t, lm, 70)
}

func makeLogKey(idx int) string {
	return fmt.Sprintf("record_%d", idx)
}

func makeLogVal(idx int) int {
	return idx + 100
}

// testLogIteration verifies that logs are returned in a LIFO manner
func testLogIteration(t *testing.T, lm *log.WalWriter, from int) {
	t.Log("The log file has now these records:")
	iter := lm.Iterator()
	f := from
	page := file.NewPage()
	for {
		if !iter.HasNext() {
			break
		}

		sexp := makeLogKey(f)
		vexp := makeLogVal(f)
		f--

		record := iter.Next()
		page.UnsafeCopyRaw(0, record)

		s := page.String(0)
		if s != sexp {
			t.Fatalf("expected key %q, got %q", vexp, s)
		}

		npos := file.StrLength(len(s))
		v := page.Int(npos)

		if v != vexp {
			t.Fatalf("expected value %d, got %d", vexp, v)
		}

		t.Logf("[%s, val: %d]", s, v)
	}
	t.Log("\n")
}

// populateLogManager appends logs of format K -> V to the logfile
func populateLogManager(t *testing.T, lm *log.WalWriter, start, end int) {
	t.Log("Creating log records:")
	page := file.NewPage()
	for i := start; i <= end; i++ {
		record := createLogRecord(page, makeLogKey(i), makeLogVal(i))
		lsn := lm.Append(record)
		t.Logf("%d", lsn)
	}
	t.Log("Records created.")
}

func createLogRecord(page *file.Page, s string, val int) []byte {
	npos := file.StrLength(len(s))
	b := make([]byte, npos+file.IntSize)
	page.SetString(0, s)
	page.SetInt(npos, val)

	copy(b, page.Slice(0, npos+file.IntSize))
	return b
}
