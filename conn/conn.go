package conn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/tx"
)

type db interface {
	Exec(x tx.Transaction, cmd sql.Command) (fmt.Stringer, error)
	NewTx() tx.Transaction
}

type toStringer string

func (s toStringer) String() string {
	return string(s)
}

func Listen(ctx context.Context, port string, db db) error {
	l, err := net.Listen("tcp4", port)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			conn, err := l.Accept()
			if err != nil {
				return err
			}

			go handleConn(ctx, conn, db)
		}
	}
}

func handleConn(ctx context.Context, conn net.Conn, db db) {
	greet(conn)

	session := session{
		id: nextSessionID(),
		db: db,
	}

	for {
		cmd, err := bufio.NewReader(conn).ReadString(';')
		if err != nil {
			fmt.Fprintf(conn, "Error reading command: %s", err)
		}
		cmd = cmd[:len(cmd)-1]
		cmd = strings.TrimSpace(cmd)

		out, err := session.processInput(ctx, cmd)

		if err == io.EOF {
			fmt.Fprint(conn, out)
			conn.Close()
			return
		}

		if err != nil {
			fmt.Fprint(conn, err)
			return
		}

		fmt.Fprint(conn, out)
		fmt.Fprint(conn, "\n> ")
	}
}

func greet(conn net.Conn) {
	const msg = "Hello user! Thanks for using SimpleDB!\n> "
	fmt.Fprint(conn, msg)
}
