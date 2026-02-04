package cluster

import (
	"sync/atomic"
)

func newCloseGuard() closeGuard {
	return closeGuard{}
}

// We don't need sync.Once here because:
//
//  1. Original sync.Once can't provide it's own state 'from the box'
//  2. We don't need a sync.Once guarantee that Close will return only after operation resolved
//     (in other terms - we don't need sync.Once.mu additional field),
//     for more information you can check https://cs.opensource.google/go/go/+/go1.25.5:src/sync/once.go;l=52
type closeGuard struct {
	closed atomic.Bool
}

func (c *closeGuard) CloseOnce(cause error, closeFunc func(cause error)) {
	if cause == nil { // unexpected
		cause = CauseUnexpectedEOF
	}
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	if closeFunc != nil {
		closeFunc(cause)
	}
}
func (c *closeGuard) IsClosed() bool {
	return c.closed.Load()
}
