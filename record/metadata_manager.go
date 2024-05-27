package record

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
	*TableManager
	*ViewManager
	*IndexManager
	*StatManager
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
