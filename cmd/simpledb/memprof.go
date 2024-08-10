//go:build memprof

package main

import (
	"fmt"
	"os"
	"runtime/pprof"
)

func init() {
	hooks = append(hooks, &memprof{})
}

type memprof struct {
	f *os.File
}

func (m *memprof) OnStart() error {
	fmt.Println("memprof hook initialised...")
	f, err := os.Create("mem.prof")
	if err != nil {
		return err
	}
	m.f = f

	return nil
}

func (m *memprof) OnEnd() error {
	fmt.Println("generating memory profile...")
	defer fmt.Println("memory profile generated")
	if err := pprof.WriteHeapProfile(m.f); err != nil {
		return err
	}
	m.f.Close()

	return nil
}
