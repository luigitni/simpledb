package tx

import (
	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
)

type BufferList struct {
	buffers map[string]*buffer.Buffer
	pins    map[string]int // holds a counter of pins
	bm      *buffer.Manager
}

func MakeBufferList(bm *buffer.Manager) BufferList {
	return BufferList{
		buffers: map[string]*buffer.Buffer{},
		pins:    map[string]int{},
		bm:      bm,
	}
}

// GetBuffer returns the the buffer pinned to the specified block.
// If such a buffer does not exists, it returns nil
// (for example, if the tx has not yet pinned the block)
func (list *BufferList) GetBuffer(block file.BlockID) *buffer.Buffer {
	return list.buffers[block.String()]
}

// Pin pins the specified block and keeps track of the buffer internally.
// Returns a buffer.ErrClientTimeOut If the buffer cannot be pinned due to none being available
func (list *BufferList) Pin(block file.BlockID) error {
	buf, err := list.bm.Pin(block)
	if err != nil {
		return err
	}
	key := block.String()
	list.buffers[key] = buf
	// increase the pinned counter
	list.pins[key]++
	return nil
}

func (list *BufferList) Unpin(block file.BlockID) {
	key := block.String()
	buf := list.buffers[key]
	// todo: handle the case in which the buffer has not been pinned yet
	list.bm.Unpin(buf)

	// decrement pins
	if c, ok := list.pins[key]; !ok || c == 1 {
		delete(list.pins, key)
		delete(list.buffers, key)
	} else {
		list.pins[key]--
	}
}

// Unpin any buffer still pinned by this transaction
func (list *BufferList) UnpinAll() {
	for k := range list.pins {
		buf := list.buffers[k]
		list.bm.Unpin(buf)
	}

	list.buffers = map[string]*buffer.Buffer{}
	list.pins = map[string]int{}
}
