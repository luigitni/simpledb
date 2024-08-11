package conn

import (
	"fmt"
	"strings"
	"time"
)

type timedResult struct {
	res      fmt.Stringer
	duration time.Duration
}

func (tr timedResult) String() string {
	var builder strings.Builder
	builder.WriteString(tr.res.String())
	builder.WriteByte('\n')
	elapsed := float64(tr.duration) / float64(time.Millisecond)
	builder.WriteString(fmt.Sprintf("(%.2f ms)", elapsed))
	return builder.String()
}
