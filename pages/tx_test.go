package pages

import (
	"math/rand"
	"sync"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

var _ tx.Transaction = &mockTx{}

type mockTx struct {
	sync.Mutex

	storage mockTxStorage

	id int

	isPinned   bool
	isCommit   bool
	isRollback bool

	commitCalls   int
	rollbackCalls int
	recoverCalls  int
	appendCalls   int

	setIntCalls    int
	setStringCalls int
	getIntCalls    int
	getStringCalls int
}

type mockTxStorage interface {
	setInt(blockID file.Block, offset int, val int, shouldLog bool) error
	setString(blockID file.Block, offset int, val string, shouldLog bool) error

	getInt(blockID file.Block, offset int) (int, error)
	getString(blockID file.Block, offset int) (string, error)
}

type defaultMockTxStorage map[file.Block]map[int]interface{}

func (s defaultMockTxStorage) getInt(blockID file.Block, offset int) (int, error) {
	om, ok := s[blockID]
	if !ok {
		return 0, nil
	}

	if v, ok := om[offset].(int); ok {
		return v, nil
	}

	return 0, nil
}

func (s defaultMockTxStorage) getString(blockID file.Block, offset int) (string, error) {
	om, ok := s[blockID]
	if !ok {
		return "", nil
	}

	if v, ok := om[offset].(string); ok {
		return v, nil
	}

	return "", nil
}

func (s defaultMockTxStorage) setInt(blockID file.Block, offset int, val int, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[int]interface{})
		s[blockID] = om
	}

	om[offset] = val

	return nil
}

func (s defaultMockTxStorage) setString(blockID file.Block, offset int, val string, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[int]interface{})
		s[blockID] = om
	}

	om[offset] = val
	return nil
}

func newMockTx() *mockTx {
	return &mockTx{
		id:      rand.Int(),
		storage: defaultMockTxStorage{},
	}
}

func newMockTxWithId(id int) *mockTx {
	mtx := newMockTx()
	mtx.id = id

	return mtx
}

func (t *mockTx) Id() int {
	return t.id
}

// Append implements tx.Transaction.
func (t *mockTx) Append(fname string) (file.Block, error) {
	t.Lock()
	defer t.Unlock()
	t.appendCalls++

	return file.Block{}, nil
}

// BlockSize implements tx.Transaction.
func (t *mockTx) BlockSize() int {
	return file.PageSize
}

// Commit implements tx.Transaction.
func (t *mockTx) Commit() {
	t.Lock()
	defer t.Unlock()
	t.isCommit = true
	t.commitCalls++
}

// Pin implements tx.Transaction.
func (t *mockTx) Pin(blockID file.Block) {
	t.Lock()
	defer t.Unlock()
	t.isPinned = true
}

// Recover implements tx.Transaction.
func (t *mockTx) Recover() {
	t.Lock()
	defer t.Unlock()
	t.recoverCalls++
}

// Rollback implements tx.Transaction.
func (t *mockTx) Rollback() {
	t.Lock()
	defer t.Unlock()
	t.isRollback = true
	t.rollbackCalls++
}

// Int implements tx.Transaction.
func (t *mockTx) Int(blockID file.Block, offset int) (int, error) {
	t.Lock()
	defer t.Unlock()

	t.getIntCalls++
	return t.storage.getInt(blockID, offset)
}

// SetInt implements tx.Transaction.
func (t *mockTx) SetInt(blockID file.Block, offset int, val int, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setIntCalls++
	return t.storage.setInt(blockID, offset, val, shouldLog)
}

// String implements tx.Transaction.
func (t *mockTx) String(blockID file.Block, offset int) (string, error) {
	t.Lock()
	defer t.Unlock()

	t.getStringCalls++
	return t.storage.getString(blockID, offset)
}

// SetString implements tx.Transaction.
func (t *mockTx) SetString(blockID file.Block, offset int, val string, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setStringCalls++
	return t.storage.setString(blockID, offset, val, shouldLog)
}

// Size implements tx.Transaction.
func (t *mockTx) Size(fname string) (int, error) {
	panic("unimplemented")
}

// Unpin implements tx.Transaction.
func (t *mockTx) Unpin(blockID file.Block) {
	t.Lock()
	defer t.Unlock()
	t.isPinned = false
}
