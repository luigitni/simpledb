package log

import (
	"sync"

	"github.com/luigitni/simpledb/file"
)

type Manager struct {
	fm           *file.Manager
	logfile      string
	logpage      *file.Page
	currentBlock file.BlockID
	latestLSN    int
	lastSavedLSN int
	sync.Mutex
}

func NewLogManager(fm *file.Manager, logfile string) *Manager {
	b := make([]byte, fm.BlockSize())

	logsize := fm.Size(logfile)

	logpage := file.NewPageWithSlice(b)

	man := &Manager{
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
		man.currentBlock = file.NewBlockID(logfile, logsize-1)
		fm.Read(man.currentBlock, logpage)
	}

	return man
}

// flush writes the contents of the logpage into the currentBlock
// and updates the lastSavedLSN id
func (man *Manager) flush() {
	man.fm.Write(man.currentBlock, man.logpage)
	man.lastSavedLSN = man.latestLSN
}

func (man *Manager) Flush(lsn int) {
	if lsn >= man.lastSavedLSN {
		man.flush()
	}
}

func (man *Manager) Iterator() *Iterator {
	man.flush()
	return newIterator(man.fm, man.currentBlock)
}

// Append adds a record to the log page.
// If the record does not fit, flushes the current contents into the current block
// and creates a new one to append data to.
// The page writes data starting from the end of the buffer and uses the first file.IntBytes to write an header
// that keeps track of where to prepend new records:
// ------
// head of the buffer -> | recpos | . . . . . . . | existing records | <- end of the buffer
//                           ^--------------------^
//                     file.IntBytes        value of recpos
// when a new record is inserted, its lenght is computed.
// if the record fits, its is prepended at the "recpos" index
// and recpos is updated.
// head of the buffer -> | recpos - sizeof(newRecord) | . . . . . .| sizeof(newRecord) | existing records | <- end of the buffer
//                                   ^-----------------------------^
//                             file.IntBytes               value of recpos
func (man *Manager) Append(records []byte) int {
	man.Lock()
	defer man.Unlock()

	// boundary contains the offset of the most recently added record
	spaceLeft := man.logpage.GetInt(0)

	recsize := len(records)

	bytesneeded := recsize + file.IntBytes

	// if the bytes needed to insert the record, PLUS the page header, are larger than the space left
	// the record won't fit.
	// In this case, flush the current page and move to the next block
	if bytesneeded+file.IntBytes > spaceLeft {
		man.flush()
		man.currentBlock = man.appendNewBlock()
		spaceLeft = man.logpage.GetInt(0)
	}

	// compute the leading byte from where the record will start
	recpos := spaceLeft - bytesneeded
	// note that the page is writing data starting from the end of the buffer
	// moving towards the head
	man.logpage.SetBytes(recpos, records)
	man.logpage.SetInt(0, recpos) // the new boundary
	man.latestLSN++
	return man.latestLSN
}

// appendNewBlock appends a new block-sized array to the logfile via the file manager and returns it's index
// It then writes the size of the block in the page first IntBytes (the page header?)
// We will use the header to keep track of where we are when prepending data to the page.
func (man *Manager) appendNewBlock() file.BlockID {
	block := man.fm.Append(man.logfile)
	man.logpage.SetInt(0, man.fm.BlockSize())

	// write the logpage into the newly created block
	man.fm.Write(block, man.logpage)
	return block
}
