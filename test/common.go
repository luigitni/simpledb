// test package includes common methods to run tests.
// It should not be included in release builds
package test

import (
	"os"
	"path"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

const dbFolder = "../test_data"
const logfile = "testlog"
const blockfile = "testfile"
const blockSize = 400
const buffersAvaialble = 3

type Conf struct {
	DbFolder         string
	LogFile          string
	BlockFile        string
	BlockSize        int
	BuffersAvailable int
}

var DefaultConfig = Conf{
	DbFolder:         dbFolder,
	LogFile:          logfile,
	BlockFile:        blockfile,
	BlockSize:        blockSize,
	BuffersAvailable: buffersAvaialble,
}

func ClearTestFolder() {
	p := path.Join(dbFolder, blockfile)
	os.Remove(p)
	p = path.Join(dbFolder, logfile)
	os.Remove(p)
}

func MakeManagers() (*file.Manager, *log.Manager, *buffer.Manager) {
	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logfile)

	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	return fm, lm, bm
}

func MakeManagersWithConfig(conf Conf) (*file.Manager, *log.Manager, *buffer.Manager) {
	fm := file.NewFileManager(conf.DbFolder, conf.BlockSize)
	lm := log.NewLogManager(fm, conf.LogFile)

	bm := buffer.NewBufferManager(fm, lm, conf.BuffersAvailable)

	return fm, lm, bm
}
