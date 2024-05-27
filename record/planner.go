package record

import (
	"errors"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

type UpdatePlanner interface {
	executeInsert(data sql.InsertCommand, x tx.Transaction) (int, error)
	executeDelete(data sql.DeleteCommand, x tx.Transaction) (int, error)
	executeUpdate(data sql.UpdateCommand, x tx.Transaction) (int, error)
	executeCreateTable(data sql.CreateTableCommand, x tx.Transaction) (int, error)
	executeCreateView(data sql.CreateViewCommand, x tx.Transaction) (int, error)
	executeCreateIndex(data sql.CreateIndexCommand, x tx.Transaction) (int, error)
}

func NewUpdatePlanner(mdm *MetadataManager) UpdatePlanner {
	return NewBasicUpdatePlanner(mdm)
}

func ExecuteDMLStatement(planner UpdatePlanner, cmd sql.Command, x tx.Transaction) (int, error) {
	if cmd.Type() != sql.CommandTypeDML {
		return 0, errors.New("invalid command type. Expected DML command")
	}

	switch c := cmd.(type) {
	case sql.InsertCommand:
		return planner.executeInsert(c, x)
	case sql.UpdateCommand:
		return planner.executeUpdate(c, x)
	case sql.DeleteCommand:
		return planner.executeDelete(c, x)
	}

	return 0, errors.New("unexpected DML command")
}

func ExecuteDDLStatement(planner UpdatePlanner, cmd sql.Command, x tx.Transaction) (int, error) {
	if cmd.Type() != sql.CommandTypeDDL {
		return 0, errors.New("invalid command type. Expected DDL command")
	}

	switch c := cmd.(type) {
	case sql.CreateTableCommand:
		return planner.executeCreateTable(c, x)
	case sql.CreateViewCommand:
		return planner.executeCreateView(c, x)
	case sql.CreateIndexCommand:
		return planner.executeCreateIndex(c, x)
	}

	return 0, errors.New("invalid command type. Expected DML command")
}
