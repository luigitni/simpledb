package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/luigitni/simpledb/conn"
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
	if err := conn.Listen(ctx, port, db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	<-quit
	canc()
	for _, h := range hooks {
		h.OnEnd()
	}
	fmt.Println("shutting down...")
}
