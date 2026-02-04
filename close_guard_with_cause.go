package cluster

import (
	"sync/atomic"
)

func newCloseGuardWithCause() closeGuardWithCause {
	return closeGuardWithCause{}
}

type closeGuardWithCause struct {
	stopCause atomic.Value // nil or non nil stop cause (error)
}

func (c *closeGuardWithCause) CloseOnce(cause error, closeFunc func(cause error)) {
	if cause == nil {
		cause = CauseUnexpectedEOF
	}
	if !c.stopCause.CompareAndSwap(nil, cause) {
		return
	}
	if closeFunc != nil {
		closeFunc(cause)
	}
}
func (c *closeGuardWithCause) IsClosed() bool {
	return c.stopCause.Load() != nil
}
func (c *closeGuardWithCause) GetStopCause() error {
	stopCause, _ := c.stopCause.Load().(error)
	return stopCause
}
