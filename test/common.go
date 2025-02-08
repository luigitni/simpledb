// test package includes common methods to run tests.
// It should not be included in release builds
package test

import (
	"testing"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/wal"
)

const (
	DefaultTestLogfile          = "testlog"
	DefaultTestBlockfile        = "testfile"
	DefautlTestBlockSize        = 400
	DefaultTestBuffersAvailable = 3
)

type Conf struct {
	DbFolder         string
	LogFile          string
	BlockFile        string
	BlockSize        storage.Long
	BuffersAvailable int
}

func DefaultConfig(t *testing.T) Conf {
	return Conf{
		DbFolder:         t.TempDir(),
		LogFile:          DefaultTestLogfile,
		BlockFile:        DefaultTestBlockfile,
		BlockSize:        DefautlTestBlockSize,
		BuffersAvailable: DefaultTestBuffersAvailable,
	}
}

func MakeManagers(t *testing.T) (*file.FileManager, *wal.WalWriter, *buffer.BufferManager) {
	fm := file.NewFileManager(t.TempDir(), DefautlTestBlockSize)
	lm := wal.NewWalWriter(fm, DefaultTestLogfile)

	bm := buffer.NewBufferManager(fm, lm, DefaultTestBuffersAvailable)

	return fm, lm, bm
}

func MakeManagersWithConfig(conf Conf) (*file.FileManager, *wal.WalWriter, *buffer.BufferManager) {
	fm := file.NewFileManager(conf.DbFolder, conf.BlockSize)
	lm := wal.NewWalWriter(fm, conf.LogFile)

	bm := buffer.NewBufferManager(fm, lm, conf.BuffersAvailable)

	return fm, lm, bm
}
