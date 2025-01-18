// test package includes common methods to run tests.
// It should not be included in release builds
package test

import (
	"testing"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
	"github.com/luigitni/simpledb/types"
)

const (
	logfile          = "testlog"
	blockfile        = "testfile"
	blockSize        = 400
	buffersAvaialble = 3
)

type Conf struct {
	DbFolder         string
	LogFile          string
	BlockFile        string
	BlockSize        types.Long
	BuffersAvailable int
}

func DefaultConfig(t *testing.T) Conf {
	return Conf{
		DbFolder:         t.TempDir(),
		LogFile:          logfile,
		BlockFile:        blockfile,
		BlockSize:        blockSize,
		BuffersAvailable: buffersAvaialble,
	}
}

func MakeManagers(t *testing.T) (*file.FileManager, *log.WalWriter, *buffer.BufferManager) {
	fm := file.NewFileManager(t.TempDir(), blockSize)
	lm := log.NewLogManager(fm, logfile)

	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	return fm, lm, bm
}

func MakeManagersWithConfig(conf Conf) (*file.FileManager, *log.WalWriter, *buffer.BufferManager) {
	fm := file.NewFileManager(conf.DbFolder, conf.BlockSize)
	lm := log.NewLogManager(fm, conf.LogFile)

	bm := buffer.NewBufferManager(fm, lm, conf.BuffersAvailable)

	return fm, lm, bm
}
