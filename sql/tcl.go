package sql

type BeginTransactionCommand struct{}

func (cmd BeginTransactionCommand) Type() CommandType {
	return CommandTypeTCLBegin
}

type CommitTransactionCommand struct{}

func (cmd CommitTransactionCommand) Type() CommandType {
	return CommandTypeTCLCommit
}

type RollbackTransactionCommand struct{}

func (cmd RollbackTransactionCommand) Type() CommandType {
	return CommandTypeTCLRollback
}

func (p Parser) isTCL() (bool, Command) {
	if p.matchKeyword("begin") {
		return true, BeginTransactionCommand{}
	}

	if p.matchKeyword("commit") {
		return true, CommitTransactionCommand{}
	}

	if p.matchKeyword("rollback") {
		return true, RollbackTransactionCommand{}
	}

	return false, nil
}
