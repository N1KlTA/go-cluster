package cluster

func newClonedSub[T any](original Sub[T], onClose func(cause error)) *clonedSub[T] {
	return &clonedSub[T]{
		original: original,
		onClose:  onClose,
		guard:    newCloseGuard(),
	}
}

type clonedSub[T any] struct {
	original Sub[T]
	onClose  func(cause error)
	guard    closeGuard
}

func (s *clonedSub[T]) TrySend(event T) error {
	if s.guard.IsClosed() {
		return ErrOutputClosed
	}
	return s.original.TrySend(event)
}
func (s *clonedSub[T]) Close(cause error) {
	s.guard.CloseOnce(cause, s.onClose)
}
func (c *clonedSub[T]) IsClosed() bool {
	return c.guard.IsClosed()
}
