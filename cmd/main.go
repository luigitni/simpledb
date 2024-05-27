package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/luigitni/simpledb/db"
)

const (
	port = ":8765"
)

func main() {

	db := db.NewDB()

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

		// parse the incoming command and feed it to the database
		out, err := db.Exec(cmd)
		if err != nil {
			fmt.Fprintln(conn, err)
		}

		fmt.Fprint(conn, out)
	}
}

func greet(conn net.Conn) {
	const msg = "Hello user! Thanks for using SimpleDB!\n"
	fmt.Fprint(conn, msg)
}
