package cluster

import "sync"

func newChannelSub[T any](channel chan<- T, callback func(cause error)) *channelSub[T] {
	if callback == nil {
		callback = func(cause error) {}
	}
	return &channelSub[T]{
		channel: channel,
		guard:   newCloseGuardWithCause(),

		callback: callback,
	}
}

// static type check
var _ Sub[int] = &channelSub[int]{}

type channelSub[T any] struct {
	channel chan<- T
	guard   closeGuardWithCause // guard from double closing
	mu      sync.Mutex          // guard from send-close races

	callback func(cause error)
}

func (s *channelSub[T]) TrySend(event T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.guard.IsClosed() {
		return ErrOutputClosed
	}
	select {
	case s.channel <- event:
		return nil
	default:
		return ErrSubOverloaded
	}
}
func (s *channelSub[T]) Close(cause error) {
	s.guard.CloseOnce(cause, func(cause error) {
		s.mu.Lock()
		close(s.channel)
		s.mu.Unlock()
		s.callback(cause)
	})
}
func (s *channelSub[T]) IsClosed() bool {
	return s.guard.IsClosed()
}
func (s *channelSub[T]) GetStopCause() error {
	return s.guard.GetStopCause()
}
