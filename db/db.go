package db

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

const (
	defaultPath      = "data"
	defaultLogFile   = "wal"
	blockSize        = 4000
	buffersAvaialble = 500
)

type DB struct {
	fm  *file.Manager
	lm  *log.Manager
	bm  *buffer.Manager
	mdm *record.MetadataManager
}

func NewDB() (*DB, error) {
	fm := file.NewFileManager(defaultPath, blockSize)
	lm := log.NewLogManager(fm, defaultLogFile)
	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	mdm := record.NewMetadataManager()

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

func (db *DB) beginTx() tx.Transaction {
	return tx.NewTx(db.fm, db.lm, db.bm)
}

// todo: define a common serialised format to return instead of a Stringer.
// To the extents of playing with the database, this is good enough for the moment.
func (db *DB) Exec(command string) (fmt.Stringer, error) {

	parser := sql.NewParser(command)

	data, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	switch data.Type() {
	case sql.CommandTypeQuery:
		return db.runQuery(data.(sql.Query))
	case sql.CommandTypeDML:
		return db.execDML(data)
	case sql.CommandTypeDDL:
		return db.execDDL(data)
	}

	return nil, errors.New("invalid command")
}

func (db *DB) runQuery(q sql.Query) (Rows, error) {
	x := db.beginTx()

	run := func() (Rows, error) {
		planner := record.NewBasicQueryPlanner(db.mdm)

		plan, err := planner.CreatePlan(q, x)
		if err != nil {
			return Rows{}, err
		}

		scan := plan.Open()
		defer scan.Close()

		var rows Rows

		rows.cols = append(rows.cols, q.Fields()...)

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
				v, err := scan.GetVal(f)
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

	rows, err := run()
	if err != nil {
		x.Rollback()
	} else {
		x.Commit()
	}

	return rows, err
}

type Result struct {
	affected int
}

func (r Result) String() string {
	return fmt.Sprintf("%d", r.affected)
}

func (db *DB) execDDL(cmd sql.Command) (Result, error) {
	planner := record.NewUpdatePlanner(db.mdm)

	x := db.beginTx()

	res, err := record.ExecuteDDLStatement(planner, cmd, x)
	if err != nil {
		x.Rollback()
	} else {
		x.Commit()
	}

	return Result{res}, err
}

func (db *DB) execDML(cmd sql.Command) (Result, error) {
	planner := record.NewUpdatePlanner(db.mdm)

	x := db.beginTx()

	res, err := record.ExecuteDMLStatement(planner, cmd, x)
	if err != nil {
		x.Rollback()
	} else {
		x.Commit()
	}

	return Result{res}, err
}
