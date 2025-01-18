package pages

import (
	"math/rand"
	"sync"

	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

var _ tx.Transaction = &mockTx{}

type mockTx struct {
	sync.Mutex

	storage mockTxStorage

	id types.TxID

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
	setFixedLen(blockID types.Block, offset types.Offset, size types.Size, val types.FixedLen, shouldLog bool) error
	setVarLen(blockID types.Block, offset types.Offset, val types.Varlen, shouldLog bool) error

	getFixedLen(blockID types.Block, offset types.Offset, size types.Size) (types.FixedLen, error)
	getVarLen(blockID types.Block, offset types.Offset) (types.Varlen, error)
}

type defaultMockTxStorage map[types.Block]map[types.Offset]interface{}

func (s defaultMockTxStorage) getFixedLen(blockID types.Block, offset types.Offset, size types.Size) (types.FixedLen, error) {
	om, ok := s[blockID]
	if !ok {
		return nil, nil
	}

	if v, ok := om[offset].(types.FixedLen); ok {
		return v, nil
	}

	return nil, nil
}

func (s defaultMockTxStorage) getVarLen(blockID types.Block, offset types.Offset) (types.Varlen, error) {
	om, ok := s[blockID]
	if !ok {
		return types.Varlen{}, nil
	}

	if v, ok := om[offset].(types.Varlen); ok {
		return v, nil
	}

	return types.Varlen{}, nil
}

func (s defaultMockTxStorage) setFixedLen(blockID types.Block, offset types.Offset, size types.Size, val types.FixedLen, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[types.Offset]interface{})
		s[blockID] = om
	}

	om[offset] = val

	return nil
}

func (s defaultMockTxStorage) setVarLen(blockID types.Block, offset types.Offset, val types.Varlen, shouldLog bool) error {
	om, ok := s[blockID]

	if !ok {
		om = make(map[types.Offset]interface{})
		s[blockID] = om
	}

	om[offset] = val
	return nil
}

func newMockTx() *mockTx {
	return &mockTx{
		id:      types.TxID(rand.Uint32()),
		storage: defaultMockTxStorage{},
	}
}

func newMockTxWithId(id types.TxID) *mockTx {
	mtx := newMockTx()
	mtx.id = id

	return mtx
}

func (t *mockTx) Id() types.TxID {
	return t.id
}

// Append implements tx.Transaction.
func (t *mockTx) Append(fname string) (types.Block, error) {
	t.Lock()
	defer t.Unlock()
	t.appendCalls++

	return types.Block{}, nil
}

// BlockSize implements tx.Transaction.
func (t *mockTx) BlockSize() types.Offset {
	return types.PageSize
}

// Commit implements tx.Transaction.
func (t *mockTx) Commit() {
	t.Lock()
	defer t.Unlock()
	t.isCommit = true
	t.commitCalls++
}

// Pin implements tx.Transaction.
func (t *mockTx) Pin(blockID types.Block) {
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
func (t *mockTx) FixedLen(blockID types.Block, offset types.Offset, size types.Size) (types.FixedLen, error) {
	t.Lock()
	defer t.Unlock()

	t.getIntCalls++
	return t.storage.getFixedLen(blockID, offset, size)
}

// SetInt implements tx.Transaction.
func (t *mockTx) SetFixedLen(blockID types.Block, offset types.Offset, size types.Size, val types.FixedLen, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setIntCalls++
	return t.storage.setFixedLen(blockID, offset, size, val, shouldLog)
}

// String implements tx.Transaction.
func (t *mockTx) VarLen(blockID types.Block, offset types.Offset) (types.Varlen, error) {
	t.Lock()
	defer t.Unlock()

	t.getStringCalls++
	return t.storage.getVarLen(blockID, offset)
}

// SetString implements tx.Transaction.
func (t *mockTx) SetVarLen(blockID types.Block, offset types.Offset, val types.Varlen, shouldLog bool) error {
	t.Lock()
	defer t.Unlock()

	t.setStringCalls++
	return t.storage.setVarLen(blockID, offset, val, shouldLog)
}

// Size implements tx.Transaction.
func (t *mockTx) Size(fname string) (types.Long, error) {
	panic("unimplemented")
}

// Unpin implements tx.Transaction.
func (t *mockTx) Unpin(blockID types.Block) {
	t.Lock()
	defer t.Unlock()
	t.isPinned = false
}
