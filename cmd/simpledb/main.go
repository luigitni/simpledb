package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/luigitni/simpledb/db"
)

const (
	port = ":8765"
)

func main() {

	db, err := db.NewDB()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// listen to incoming tcp connections
	l, err := net.Listen("tcp4", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleSession(conn, db)
	}
}

type timedResult struct {
	res      fmt.Stringer
	duration time.Duration
}

func (tr timedResult) String() string {
	var builder strings.Builder
	builder.WriteString(tr.res.String())
	builder.WriteByte('\n')
	elapsed := float64(tr.duration) / float64(time.Millisecond)
	builder.WriteString(fmt.Sprintf("(%.2f ms)", elapsed))
	return builder.String()
}

func handleSession(conn net.Conn, db *db.DB) {
	greet(conn)

	for {
		cmd, err := bufio.NewReader(conn).ReadString(';')
		cmd = cmd[:len(cmd)-1]
		cmd = strings.TrimSpace(cmd)

		switch cmd {
		case "exit":
			fmt.Fprint(conn, "bye!\n")
			conn.Close()
			return
		}

		if err != nil {
			fmt.Println(err)
			return
		}

		start := time.Now()
		res, err := db.Exec(cmd)
		if err != nil {
			fmt.Fprintln(conn, err)
		}

		out := timedResult{
			res:      res,
			duration: time.Since(start),
		}

		fmt.Fprint(conn, out)
		fmt.Fprint(conn, "\n> ")
	}
}

func greet(conn net.Conn) {
	const msg = "Hello user! Thanks for using SimpleDB!\n> "
	fmt.Fprint(conn, msg)
}
