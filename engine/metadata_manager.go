package engine

import (
	"github.com/luigitni/simpledb/tx"
)

// MetadataManager is the component of SimpleDB that
// stores and retrieves its metadata.
//   - Table metadata describes the structure of the records of a table
//     for example type, length, and offset from each record.
//   - View metadata specifies the properties of each view, like its definition.
//   - Index metadata keeps track of which fields in which table have an index associated
//     so that they can be possibly used in planning.
//   - Stats metadata describes the size of each table and the distribution of its
//     field values.
type MetadataManager struct {
	*tableManager
	*viewManager
	*indexManager
	*statManager
}

func NewMetadataManager() *MetadataManager {
	tm := newTableManager()
	sm := newStatManager(tm)
	im := newIndexManager(tm, sm)
	vm := newViewManager(tm)

	return &MetadataManager{
		tableManager: tm,
		viewManager:  vm,
		indexManager: im,
		statManager:  sm,
	}
}

func (man *MetadataManager) Init(trans tx.Transaction) error {
	if err := man.tableManager.init(trans); err != nil {
		return err
	}

	if err := man.viewManager.init(trans); err != nil {
		return err
	}

	if err := man.statManager.init(trans); err != nil {
		return err
	}

	if err := man.indexManager.init(trans); err != nil {
		return err
	}

	return nil
}
