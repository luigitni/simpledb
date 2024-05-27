package buffer

import "errors"

var ErrClientTimeout = errors.New("client has timed out while waiting for buffers to free")
