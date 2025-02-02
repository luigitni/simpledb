package log

import (
	"sync"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
)

type WalWriter struct {
	fm           *file.FileManager
	logfile      string
	logpage      *storage.Page
	currentBlock storage.Block
	latestLSN    int
	lastSavedLSN int
	sync.Mutex
}

func NewLogManager(fm *file.FileManager, logfile string) *WalWriter {
	logsize := fm.Size(logfile)

	logpage := storage.NewPage()

	man := &WalWriter{
		fm:           fm,
		logfile:      logfile,
		logpage:      logpage,
		latestLSN:    0,
		lastSavedLSN: 0,
	}

	if logsize == 0 {
		// empty log, create a new one
		man.currentBlock = man.appendNewBlock()
	} else {
		man.currentBlock = storage.NewBlock(logfile, logsize-1)
		fm.Read(man.currentBlock, logpage)
	}

	return man
}

// flush writes the contents of the WAL page into the currentBlock
// and updates the lastSavedLSN id
func (man *WalWriter) flush() {
	man.fm.Write(man.currentBlock, man.logpage)
	man.lastSavedLSN = man.latestLSN
}

// Flush compares the requested Log Sequence Number
// with the latest that has been flushed to disk.
// If the requested LSN is greater than the latest dumped
// we need to access the disk and flush.
func (man *WalWriter) Flush(lsn int) {
	if lsn >= man.lastSavedLSN {
		man.flush()
	}
}

func (man *WalWriter) Iterator() *WalIterator {
	man.flush()
	p := iteratorPool.Get().(*storage.Page)
	return newWalIterator(p, man.fm, man.currentBlock)
}

// Append adds a record to the log page.
// If the record does not fit, flushes the current contents into the current block
// and creates a new one to append data to.
// The page writes data starting from the end of the buffer and uses the first file.IntBytes to write an header
// that keeps track of where to prepend new records:
// ------
// head of the buffer -> | recpos | . . . . . . . | existing records | <- end of the buffer
//
//	      ^--------------------^
//	file.IntBytes        value of recpos
//
// when a new record is inserted, its lenght is computed.
// if the record fits, its is prepended at the "recpos" index
// and recpos is updated.
// head of the buffer -> | recpos - sizeof(newRecord) | . . . . . .| sizeof(newRecord) | existing records | <- end of the buffer
//
//	      ^-----------------------------^
//	file.IntBytes               value of recpos
func (man *WalWriter) Append(records []byte) int {
	man.Lock()
	defer man.Unlock()

	// boundary contains the offset of the most recently added record
	spaceLeft := man.logpage.UnsafeGetFixedLen(0, storage.SizeOfOffset).UnsafeAsOffset()

	recsize := storage.Offset(len(records))

	bytesneeded := recsize + storage.Offset(storage.SizeOfInt)

	// if the bytes needed to insert the record, PLUS the page header, are larger than the space left
	// the record won't fit.
	// In this case, flush the current page and move to the next block
	if bytesneeded+storage.Offset(storage.SizeOfOffset) > spaceLeft {
		man.flush()
		man.currentBlock = man.appendNewBlock()
		spaceLeft = man.logpage.UnsafeGetFixedLen(0, storage.SizeOfOffset).UnsafeAsOffset()
	}

	// compute the leading byte from where the record will start
	recpos := spaceLeft - bytesneeded
	// note that the page is writing data starting from the end of the buffer
	// moving towards the head
	man.logpage.UnsafeWriteRawVarlen(recpos, records)

	// update the header with the new position of the record
	man.logpage.UnsafeSetFixedLen(0,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, recpos),
	)

	// todo: write the LSN into the record
	// do this when reworking the WAL
	man.latestLSN++
	return man.latestLSN
}

// appendNewBlock appends a new block-sized array to the logfile via the file manager and returns it's index
// It then writes the size of the block in the page first IntBytes (the page header?)
// We will use the header to keep track of where we are when prepending data to the page.
func (man *WalWriter) appendNewBlock() storage.Block {
	block := man.fm.Append(man.logfile)

	// write the size of the block into the page header
	man.logpage.UnsafeSetFixedLen(
		0,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, man.fm.BlockSize()),
	)

	// write the logpage into the newly created block
	man.fm.Write(block, man.logpage)
	return block
}
