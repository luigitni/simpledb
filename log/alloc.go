package log

import (
	"sync"

	"github.com/luigitni/simpledb/file"
)

var iteratorPool = sync.Pool{
	New: func() any {
		return file.NewPage()
	},
}
