package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

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
	for {
		cmd, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		// parse the incoming command and feed it to the database
		out, err := db.Exec(cmd)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Fprint(os.Stdout, out)
	}
}
