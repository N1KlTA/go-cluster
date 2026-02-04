package cluster

import (
	"context"
	"sync"
)

func newChannelConsumer[T any](channel chan<- T, stopCallback func(cause error)) *channelConsumer[T] {
	return &channelConsumer[T]{
		channel:     channel,
		closeSignal: make(chan struct{}),
		guard:       newCloseGuardWithCause(),

		stopCallback: stopCallback,
	}
}

// static type check
var _ CustomConsumer[int] = &channelConsumer[int]{}

type channelConsumer[T any] struct {
	channel     chan<- T            // guraded by mu and guard
	closeSignal chan struct{}       // to avoid blocking on send, possible overhead but seems non critical
	guard       closeGuardWithCause // guards closeSignal's and channel's closes from double closing
	mu          sync.RWMutex        // guards channel from send-close race

	stopCallback func(cause error)
}

func (c *channelConsumer[T]) Send(ctx context.Context, msg T) error {
	if !c.mu.TryRLock() { // being closed
		return ErrOutputClosed
	}
	defer c.mu.RUnlock()
	if c.guard.IsClosed() {
		return ErrOutputClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeSignal: // to unlock mutex
		return ErrOutputClosed
	case c.channel <- msg:
		return nil
	}
}
func (c *channelConsumer[T]) Close(cause error) {
	c.guard.CloseOnce(cause, func(cause error) {
		close(c.closeSignal) // push senders to unlock c.mu
		c.mu.Lock()
		close(c.channel) // close original channel under mutex
		c.mu.Unlock()
		c.stopCallback(cause) // activate provided callback
	})
}
func (c *channelConsumer[T]) IsClosed() bool {
	return c.guard.IsClosed()
}
func (c *channelConsumer[T]) GetStopCause() error {
	return c.guard.GetStopCause()
}
