package tx

import (
	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/storage"
)

type bufferList struct {
	buffers map[storage.BlockID]*buffer.Buffer
	pins    map[storage.BlockID]int // holds a counter of pins
	bm      *buffer.BufferManager
}

func makeBufferList(bm *buffer.BufferManager) bufferList {
	return bufferList{
		buffers: map[storage.BlockID]*buffer.Buffer{},
		pins:    map[storage.BlockID]int{},
		bm:      bm,
	}
}

// buffer returns the buffer pinned to the specified block.
// If such a buffer does not exists, it returns nil
// (for example, if the tx has not yet pinned the block)
func (list *bufferList) buffer(block storage.Block) (*buffer.Buffer, error) {
	buf := list.buffers[block.ID()]

	// the buffer has been replaced by the buffer manager
	if buf == nil || buf.Block().ID() != block.ID() {
		delete(list.buffers, block.ID())
		delete(list.pins, block.ID())

		return list.pin(block)
	}

	return buf, nil
}

// pin pins the specified block and keeps track of the buffer internally.
// Returns a buffer.ErrClientTimeOut If the buffer cannot be pinned due to none being available
func (list *bufferList) pin(block storage.Block) (*buffer.Buffer, error) {
	buf, err := list.bm.Pin(block)
	if err != nil {
		return nil, err
	}

	key := block.ID()
	list.buffers[key] = buf
	list.pins[key]++

	return buf, nil
}

func (list *bufferList) unpin(block storage.Block) {
	key := block.ID()
	buf := list.buffers[key]

	list.bm.Unpin(buf)

	if c, ok := list.pins[key]; !ok || c == 1 {
		delete(list.pins, key)
		delete(list.buffers, key)
	} else {
		list.pins[key]--
	}
}

func (list *bufferList) unpinAll() {
	for k := range list.pins {
		buf := list.buffers[k]
		list.bm.Unpin(buf)
	}

	clear(list.buffers)
	clear(list.pins)
}
