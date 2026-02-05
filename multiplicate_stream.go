package cluster

import (
	"context"
	"fmt"
	"sync"
)

// New stream will subscribe all subs to one session and multiplicate updates
func MultiplicateStream[T any](stream Stream[T]) Stream[T] {
	if _, alreadyMultiplicated := stream.(*multiplicatedStream[T]); alreadyMultiplicated {
		return stream
	}
	return &multiplicatedStream[T]{
		originalStream: stream,
		currentStop:    func() {},
	}
}

type multiplicatedStream[T any] struct {
	originalStream       Stream[T]        // immutable
	currentMultiplicator Multiplicator[T] // protected by mu
	currentStop          func()           // protected by mu
	// new connection only under mu
	mu sync.Mutex
}

func (s *multiplicatedStream[T]) Subscribe(ctx context.Context, sub Sub[T]) (context.CancelFunc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentMultiplicator == nil {
		if err := s.start(ctx); err != nil {
			sub.Close(CauseCanceled)
			return NilCancel, err
		}
	}
	localStop, err := s.currentMultiplicator.AsStream().Subscribe(ctx, sub)
	if err != nil {
		sub.Close(CauseCanceled)
		return NilCancel, err
	}
	actualStop := func() {
		localStop()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stopIfNeeded()
	}
	return actualStop, nil
}

// should be called under mu
func (s *multiplicatedStream[T]) stopIfNeeded() {
	if s.currentMultiplicator == nil {
		return
	}
	if s.currentMultiplicator.IsClosed() || s.currentMultiplicator.GetListenersNum() == 0 {
		s.stop()
	}
}

// should be called under mu
func (s *multiplicatedStream[T]) start(ctx context.Context) error {
	if s.currentMultiplicator != nil {
		return fmt.Errorf("logic error: must stop current session first")
	}
	newMultiplicator := NewMultiplicator[T]()
	newStop, err := s.originalStream.Subscribe(ctx, newMultiplicator.AsSub())
	if err != nil {
		newMultiplicator.AsSub().Close(CauseCanceled) // just to make sure, optional
		return err
	}
	s.currentMultiplicator = newMultiplicator
	s.currentStop = newStop
	return nil
}

// should be called under mu
func (s *multiplicatedStream[T]) stop() {
	if s.currentMultiplicator == nil {
		return
	}
	s.currentStop()
	s.currentMultiplicator = nil
	s.currentStop = func() {}
}
