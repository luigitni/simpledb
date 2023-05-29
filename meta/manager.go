package meta

import (
	"github.com/luigitni/simpledb/tx"
)

type Manager struct {
	*TableManager
	*ViewManager
	*StatManager
}

func NewManager() *Manager {
	tm := NewTableManager()
	return &Manager{
		TableManager: tm,
		ViewManager:  NewViewManager(tm),
		StatManager:  NewStatManager(tm),
	}
}

func (man *Manager) Init(trans tx.Transaction) error {
	man.TableManager.Init(trans)
	man.ViewManager.Init(trans)
	return man.StatManager.Init(trans)
}
