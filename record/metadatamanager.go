package record

import (
	"github.com/luigitni/simpledb/tx"
)

type MetadataManager struct {
	*TableManager
	*ViewManager
	*StatManager
	*IndexManager
}

func NewMetadataManager() *MetadataManager {
	tm := NewTableManager()
	return &MetadataManager{
		TableManager: tm,
		ViewManager:  NewViewManager(tm),
		StatManager:  NewStatManager(tm),
	}
}

func (man *MetadataManager) Init(trans tx.Transaction) error {
	man.TableManager.Init(trans)
	man.ViewManager.Init(trans)
	return man.StatManager.Init(trans)
}
