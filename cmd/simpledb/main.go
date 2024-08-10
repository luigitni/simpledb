package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/luigitni/simpledb/db"
)

const (
	port = ":8765"
)

type hook interface {
	OnStart() error
	OnEnd() error
}

var hooks []hook

func main() {

	for _, h := range hooks {
		if err := h.OnStart(); err != nil {
			fmt.Printf("error starting hook: %s", err)
			os.Exit(1)
		}
	}

	db, err := db.NewDB()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, canc := context.WithCancel(context.Background())
	go listen(ctx, port, db)

	<-quit
	canc()
	for _, h := range hooks {
		h.OnEnd()
	}
	fmt.Println("shutting down...")
}

func listen(ctx context.Context, port string, db *db.DB) {
	l, err := net.Listen("tcp4", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}

			go handleSession(conn, db)
		}
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
