package log

import (
	"sync"

	"github.com/luigitni/simpledb/storage"
)

var iteratorPool = sync.Pool{
	New: func() any {
		return storage.NewPage()
	},
}
