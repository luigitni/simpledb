package db

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/engine"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/wal"
)

const (
	defaultPath      = "../data"
	defaultLogFile   = "../data/" + file.WALPath
	blockSize        = storage.PageSize
	buffersAvaialble = 500
)

type DB struct {
	fm  *file.FileManager
	lm  *wal.WalWriter
	bm  *buffer.BufferManager
	mdm *engine.MetadataManager
}

func NewDB() (*DB, error) {
	fm := file.NewFileManager(defaultPath, blockSize)
	lm := wal.NewWalWriter(fm, defaultLogFile)
	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	mdm := engine.NewMetadataManager()

	if fm.IsNew() {
		fmt.Println("initialising new database")
		if err := mdm.Init(x); err != nil {
			return nil, err
		}

	} else {
		fmt.Println("recovering existing database")
		x.Recover()
	}

	return &DB{
		fm:  fm,
		lm:  lm,
		bm:  bm,
		mdm: mdm,
	}, nil
}

func (db *DB) Close() {
	db.fm.Close()
}

func (db *DB) NewTx() tx.Transaction {
	return tx.NewTx(db.fm, db.lm, db.bm)
}

// todo: define a common serialised format to return instead of a Stringer.
// To the extents of playing with the database, this is good enough for the moment.
func (db *DB) Exec(x tx.Transaction, cmd sql.Command) (fmt.Stringer, error) {
	switch cmd.Type() {
	case sql.CommandTypeQuery:
		return db.RunQuery(x, cmd.(sql.Query))
	case sql.CommandTypeDML:
		return db.ExecDML(x, cmd)
	case sql.CommandTypeDDL:
		return db.ExecDDL(x, cmd)
	}

	return nil, errors.New("invalid command")
}

func (db *DB) RunQuery(x tx.Transaction, q sql.Query) (fmt.Stringer, error) {
	run := func() (Rows, error) {
		planner := engine.NewHeuristicsQueryPlanner(db.mdm)

		plan, err := planner.CreatePlan(q, x)
		if err != nil {
			return Rows{}, err
		}

		scan, err := plan.Open()
		if err != nil {
			return Rows{}, err
		}

		defer scan.Close()

		var rows Rows

		schema := plan.Schema()

		for _, f := range q.Fields() {
			rows.cols = append(rows.cols, Col{
				Name: f,
				Type: schema.FieldInfo(f).Type,
			})
		}

		for {
			err := scan.Next()
			if err == io.EOF {
				break
			}

			if err != nil {
				return Rows{}, err
			}

			row := Row{}
			for _, f := range q.Fields() {
				v, err := scan.Val(f)
				if err != nil {
					return Rows{}, err
				}

				row.vals = append(row.vals, v)
			}

			rows.rows = append(rows.rows, row)

		}

		if len(rows.rows) == 0 {
			return Rows{}, nil
		}

		return rows, nil
	}

	return run()
}

type Result struct {
	affected int
}

func (r Result) String() string {
	return fmt.Sprintf("%d", r.affected)
}

func (db *DB) ExecDDL(x tx.Transaction, cmd sql.Command) (fmt.Stringer, error) {
	planner := engine.NewUpdatePlanner(db.mdm)

	res, err := engine.ExecuteDDLStatement(planner, cmd, x)
	if err != nil {
		return Result{}, err
	}

	return Result{res}, err
}

func (db *DB) ExecDML(x tx.Transaction, cmd sql.Command) (fmt.Stringer, error) {
	planner := engine.NewUpdatePlanner(db.mdm)

	res, err := engine.ExecuteDMLStatement(planner, cmd, x)
	if err != nil {
		return Result{}, err
	}

	return Result{res}, err
}
