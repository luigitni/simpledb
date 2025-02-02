package pages

import (
	"math/rand"
	"sync"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

var _ tx.Transaction = &mockTx{}

type mockTx struct {
	sync.Mutex

	storage mockTxStorage

	id storage.TxID

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
	setFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen, shouldLog bool) error
	setVarLen(blockID storage.Block, offset storage.Offset, val storage.Varlen, shouldLog bool) error

	getFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size) (storage.FixedLen, error)
	getVarLen(blockID storage.Block, offset storage.Offset) (storage.Varlen, error)
}

type defaultMockTxStorage map[storage.Block]map[storage.Offset]interface{}

func (s defaultMockTxStorage) getFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size) (storage.FixedLen, error) {
	om, ok := s[blockID]
	if !ok {
		return nil, nil
	}

	if v, ok := om[offset].(storage.FixedLen); ok {
		return v, nil
	}

	return nil, nil
}

func (s defaultMockTxStorage) getVarLen(blockID storage.Block, offset storage.Offset) (storage.Varlen, error) {
	om, ok := s[blockID]
	if !ok {
		return storage.Varlen{}, nil
	}

	if v, ok := om[offset].(storage.Varlen); ok {
		return v, nil
	}

	return storage.Varlen{}, nil
}

func (s defaultMockTxStorage) setFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[storage.Offset]interface{})
		s[blockID] = om
	}

	om[offset] = val

	return nil
}

func (s defaultMockTxStorage) setVarLen(blockID storage.Block, offset storage.Offset, val storage.Varlen, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[storage.Offset]interface{})
		s[blockID] = om
	}

	om[offset] = val
	return nil
}

func newMockTx() *mockTx {
	return &mockTx{
		id:      storage.TxID(rand.Uint32()),
		storage: defaultMockTxStorage{},
	}
}

func newMockTxWithId(id storage.TxID) *mockTx {
	mtx := newMockTx()
	mtx.id = id

	return mtx
}

func (t *mockTx) Id() storage.TxID {
	return t.id
}

// Append implements tx.Transaction.
func (t *mockTx) Append(fname string) (storage.Block, error) {
	t.Lock()
	defer t.Unlock()
	t.appendCalls++

	return storage.Block{}, nil
}

// BlockSize implements tx.Transaction.
func (t *mockTx) BlockSize() storage.Offset {
	return storage.PageSize
}

// Commit implements tx.Transaction.
func (t *mockTx) Commit() {
	t.Lock()
	defer t.Unlock()
	t.isCommit = true
	t.commitCalls++
}

// Pin implements tx.Transaction.
func (t *mockTx) Pin(blockID storage.Block) {
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
func (t *mockTx) FixedLen(blockID storage.Block, offset storage.Offset, size storage.Size) (storage.FixedLen, error) {
	t.Lock()
	defer t.Unlock()

	t.getIntCalls++
	return t.storage.getFixedLen(blockID, offset, size)
}

// SetInt implements tx.Transaction.
func (t *mockTx) SetFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setIntCalls++
	return t.storage.setFixedLen(blockID, offset, size, val, shouldLog)
}

// String implements tx.Transaction.
func (t *mockTx) VarLen(blockID storage.Block, offset storage.Offset) (storage.Varlen, error) {
	t.Lock()
	defer t.Unlock()

	t.getStringCalls++
	return t.storage.getVarLen(blockID, offset)
}

// SetString implements tx.Transaction.
func (t *mockTx) SetVarLen(blockID storage.Block, offset storage.Offset, val storage.Varlen, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setStringCalls++
	return t.storage.setVarLen(blockID, offset, val, shouldLog)
}

// Size implements tx.Transaction.
func (t *mockTx) Size(fname string) (storage.Long, error) {
	panic("unimplemented")
}

// Unpin implements tx.Transaction.
func (t *mockTx) Unpin(blockID storage.Block) {
	t.Lock()
	defer t.Unlock()
	t.isPinned = false
}
