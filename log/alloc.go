package log

import (
	"sync"

	"github.com/luigitni/simpledb/types"
)

var iteratorPool = sync.Pool{
	New: func() any {
		return types.NewPage()
	},
}
