//go:build cpuprof

package main

import (
	"fmt"
	"os"
	"runtime/pprof"
)

func init() {
	hooks = append(hooks, &cpuprof{})
}

type cpuprof struct {
	f *os.File
}

func (c *cpuprof) OnStart() error {
	fmt.Println("cpuprof hook initialised...")
	f, err := os.Create("cpu.prof")
	if err != nil {
		return err
	}
	c.f = f

	fmt.Println("starting CPU profiling...")
	if err := pprof.StartCPUProfile(c.f); err != nil {
		return err
	}

	return nil
}

func (c *cpuprof) OnEnd() error {
	fmt.Println("stopping CPU profiling...")
	pprof.StopCPUProfile()
	c.f.Close()

	return nil
}
