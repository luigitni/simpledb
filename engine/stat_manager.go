package engine

import (
	"io"
	"sync"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

type statInfo struct {
	blocks  storage.Long
	records int
}

func (si statInfo) distinctValues(fieldName string) int {
	return 1 + si.records/3 // todo: this is just a stub
}

// statManager manages the statistical information about each table
// in the database, for example how many records the table has and
// how are they distributed.
// This information is used by the query planner to estimate plans cost
// and pick the most efficient one.
// SimpleDB StateManager is very simple and only keeps track of the followings:
// - The number of blocks used by each table
// - The number of records in each table
// - For each field of a table, the number of distinct field values it contains.
// statManager keeps the statistics of the engine in memory, in the tableStats map.
// It holds cost information for each table.
// The statInfo method returns the statstics for the requested table and every so often
// prompts a recomputation of the cost values.
type statManager struct {
	*tableManager
	sync.Mutex

	tableStats map[string]statInfo
	calls      int
}

func newStatManager(tm *tableManager) *statManager {
	return &statManager{
		tableManager: tm,
		tableStats:   map[string]statInfo{},
	}
}

func (sm *statManager) init(trans tx.Transaction) error {
	return sm.refreshStatistics(trans)
}

// statsMinCallsToRefresh is the number of calls to statInfo after which
// stats are recomputed.
const statsMinCallsToRefresh = 1000

// statInfo returns the statistics for the specified table.
// After statsMinCallsToRefresh invocations, it prompts an update of the statistics
// for all the tables.
// This is highly inefficient.
func (sm *statManager) statInfo(tname string, layout Layout, trans tx.Transaction) (statInfo, error) {
	sm.Lock()
	defer sm.Unlock()

	sm.calls++

	if sm.calls > statsMinCallsToRefresh {
		if err := sm.refreshStatistics(trans); err != nil {
			return statInfo{}, err
		}
	}

	si, ok := sm.tableStats[tname]
	if !ok {
		s, err := sm.calcTableStats(tname, layout, trans)
		if err != nil {
			return statInfo{}, err
		}
		sm.tableStats[tname] = s

		si = s
	}

	return si, nil
}

// refreshStatistics is invoked by statInfo. It reads the table catalogue and
// re-computes the statInfo object for each table.
func (sm *statManager) refreshStatistics(x tx.Transaction) error {
	sm.calls = 0
	stats := map[string]statInfo{}

	tcat, err := sm.layout(tableCatalogTableName, x)
	if err != nil {
		return err
	}

	ts := newTableScan(x, tableCatalogTableName, tcat)
	defer ts.Close()
	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		tname, err := ts.Val(catFieldTableName)
		if err != nil {
			return err
		}

		n := tname.AsGoString()

		layout, err := sm.layout(n, x)
		if err != nil {
			return err
		}

		si, err := sm.calcTableStats(n, layout, x)
		if err != nil {
			return err
		}

		stats[n] = si
	}

	sm.tableStats = stats

	return nil
}

// calcTableStats scans the whole provided table to count records and blocks and refresh the statInfo
// for the provided table
func (sm *statManager) calcTableStats(tname string, layout Layout, trans tx.Transaction) (statInfo, error) {
	var recs int
	var blocks storage.Long
	ts := newTableScan(trans, tname, layout)
	defer ts.Close()

	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return statInfo{}, err
		}

		recs++
		blocks = ts.GetRID().Blocknum + 1
	}

	return statInfo{
		blocks:  blocks,
		records: recs,
	}, nil
}
