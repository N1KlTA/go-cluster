package cluster

import (
	"context"
	"errors"
	"sync"
)

// Common multiplicator errors
var (
	ErrNoReceivers             = errors.New("no active receivers connected")          // from sub
	ErrAllFailed               = errors.New("all receivers failed")                   // from sub
	ErrMStreamSubLimitExceeded = errors.New("new sub will break multiplicator limit") // from stream
	ErrMStreamClosed           = errors.New("multiplicator closed")                   // from stream
)

// Multiplicator works as a sub group and sends copy of an update to each connected sub
//
// As long as there are some usages of Multiplicator, in many cases it is better to use MultiplicateStream(stream) instead.
// The reason is simple - it will track reconnections and will stop original stream if listeners num will fall to zero
type Multiplicator[T any] interface {
	// Sub-like interface for sending events to subscribers
	// Will return an error on TrySend only if no listeners recieved an event
	AsSub() Sub[T]
	// Stream interface for subscribing new listeners
	AsStream() Stream[T]
	// Returns current number of listeners
	GetListenersNum() int
	// Return Multiplicator state, IsClosed() = true means that event publisher permanently disconnected
	IsClosed() bool
}

// Creates a new multiplicator
//
// As long as there are some usages of Multiplicator, in many cases it is better to use MultiplicateStream(stream) instead.
// The reason is simple - it will track reconnections and will stop original stream if listeners num will fall to zero
func NewMultiplicator[T any]() Multiplicator[T] {
	return &multiplicator[T]{}
}

type multiplicator[T any] struct {
	core    multiplicatorAtomicCore[T] // invariants protected by mu
	closed  bool                       // protected by mu
	nextKey int                        // protected by mu
	mu      sync.Mutex
}

func (c *multiplicator[T]) AsSub() Sub[T] {
	return (*multiplicatorSub[T])(c)
}
func (c *multiplicator[T]) AsStream() Stream[T] {
	return (*multiplicatorStream[T])(c)
}
func (c *multiplicator[T]) GetListenersNum() int {
	return c.core.get().size()
}
func (r *multiplicator[T]) IsClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

type multiplicatorSub[T any] multiplicator[T]

func (r *multiplicatorSub[T]) TrySend(message T) error {
	core := r.core.get()
	coreSize := core.size()
	if coreSize == 0 {
		return ErrNoReceivers
	}
	errs := core.trySendToAll(message) // TODO: log?
	if len(errs) == coreSize {
		return ErrAllFailed
	}
	return nil
}
func (r *multiplicatorSub[T]) Close(cause error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.core.set(r.core.get().close(cause))
	r.closed = true
}
func (r *multiplicatorSub[T]) IsClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

type multiplicatorStream[T any] multiplicator[T]

func (r *multiplicatorStream[T]) Subscribe(ctx context.Context, output Sub[T]) (context.CancelFunc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return func() {}, ErrMStreamClosed
	}
	core := r.core.get()
	if core.size() == MultiplicatorReceiversLimit {
		return func() {}, ErrMStreamSubLimitExceeded
	}
	newKey := core.nextAvailableKey(r.nextKey)
	r.core.set(core.add(newKey, output))
	r.nextKey = newKey + 1
	return sync.OnceFunc(func() {
		r.core.set(r.core.get().remove(newKey, CauseCanceled))
	}), nil
}
