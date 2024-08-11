package conn

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

var currentSessionID int64

func nextSessionID() int {
	v := atomic.AddInt64(&currentSessionID, 1)
	return int(v)
}

type sessionState uint8

const (
	sessionStateReady sessionState = iota
	sessionStateInTx
)

type sessionMode uint8

const (
	sessionModeDefault sessionMode = iota
	sessionModeStreamed
)

type session struct {
	id        int
	db        db
	mode      sessionMode
	state     sessionState
	currentTx tx.Transaction
}

func (s *session) beginTx() error {
	if s.state == sessionStateInTx {
		return fmt.Errorf("transaction already in progress")
	}

	s.mode = sessionModeStreamed
	s.state = sessionStateInTx
	s.currentTx = s.db.NewTx()

	return nil
}

func (s *session) commitTx() error {
	if s.state != sessionStateInTx {
		return fmt.Errorf("no transaction in progress")
	}

	s.currentTx.Commit()

	s.mode = sessionModeDefault
	s.state = sessionStateReady
	s.currentTx = nil

	return nil
}

func (s *session) rollbackTx() error {
	if s.state != sessionStateInTx {
		return fmt.Errorf("no transaction in progress")
	}

	s.currentTx.Rollback()
	s.mode = sessionModeDefault
	s.state = sessionStateReady
	s.currentTx = nil

	return nil
}

func (s *session) tx() tx.Transaction {
	if s.state == sessionStateInTx {
		return s.currentTx
	}

	s.currentTx = s.db.NewTx()
	s.state = sessionStateInTx

	return s.currentTx
}

func (s *session) processInput(ctx context.Context, cmd string) (fmt.Stringer, error) {
	switch cmd {
	case "exit":
		return toStringer("bye!\n"), io.EOF
	default:
		start := time.Now()

		parser := sql.NewParser(cmd)

		data, err := parser.Parse()
		if err != nil {
			return nil, err
		}

		switch data.Type() {
		case sql.CommandTypeTCLBegin:
			if err := s.beginTx(); err != nil {
				return nil, err
			}

			return toStringer("OK\n"), nil
		case sql.CommandTypeTCLCommit:
			if err := s.commitTx(); err != nil {
				return nil, err
			}

			return toStringer("OK\n"), nil
		case sql.CommandTypeTCLRollback:
			if err := s.rollbackTx(); err != nil {
				return nil, err
			}

			return toStringer("OK\n"), nil
		default:
			x := s.tx()

			res, err := s.db.Exec(x, data)
			if err != nil {
				if s.mode == sessionModeDefault {
					s.rollbackTx()
				}

				return nil, err
			}

			if s.mode == sessionModeDefault {
				s.commitTx()
			}

			return timedResult{
				res:      res,
				duration: time.Since(start),
			}, nil
		}
	}
}
