package record

import (
	"io"
	"sync"

	"github.com/luigitni/simpledb/tx"
)

type StatInfo struct {
	Blocks  int
	Records int
}

func (si StatInfo) DistinctValues(fieldName string) int {
	return 1 + si.Records/3 // todo: this is just a stub
}

type StatManager struct {
	*TableManager
	sync.Mutex

	tableStats map[string]StatInfo
	calls      int
}

func NewStatManager(tm *TableManager) *StatManager {
	return &StatManager{
		TableManager: tm,
		tableStats:   map[string]StatInfo{},
	}
}

func (sm *StatManager) Init(trans tx.Transaction) error {
	return sm.refreshStatistics(trans)
}

func (sm *StatManager) StatInfo(tname string, layout Layout, trans tx.Transaction) (StatInfo, error) {
	sm.Lock()
	defer sm.Unlock()

	sm.calls++

	if sm.calls > 100 {
		if err := sm.refreshStatistics(trans); err != nil {
			return StatInfo{}, err
		}
	}

	si, ok := sm.tableStats[tname]
	if !ok {
		s, err := sm.calcTableStats(tname, layout, trans)
		if err != nil {
			return StatInfo{}, err
		}
		sm.tableStats[tname] = s

		si = s
	}

	return si, nil
}

func (sm *StatManager) refreshStatistics(trans tx.Transaction) error {

	sm.calls = 0
	stats := map[string]StatInfo{}

	tcat, err := sm.Layout("tblcat", trans)
	if err != nil {
		return err
	}

	ts := NewTableScan(trans, "tblcat", tcat)
	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		tname, err := ts.GetString("tblname")
		if err != nil {
			return err
		}

		layout, err := sm.Layout(tname, trans)
		if err != nil {
			return err
		}

		si, err := sm.calcTableStats(tname, layout, trans)
		if err != nil {
			return err
		}

		stats[tname] = si
	}

	sm.tableStats = stats
	ts.Close()

	return nil
}

func (sm *StatManager) calcTableStats(tname string, layout Layout, trans tx.Transaction) (StatInfo, error) {
	var recs, blocks int
	ts := NewTableScan(trans, tname, layout)
	for {
		err := ts.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return StatInfo{}, err
		}

		recs++
		blocks = ts.GetRID().Blocknum + 1
	}
	ts.Close()
	return StatInfo{
		Blocks:  blocks,
		Records: recs,
	}, nil
}
