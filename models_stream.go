package cluster

import "context"

// This file contains basic interfaces for working with event streams

// Stream is an event stream to subscribe and get notifies about updates
//
//   - Stream can skip updates after deliver fail (can retry if you want, TrySend error means no one receives any updates)
//   - Stream shouldn't track sub state
//   - Stream subscribe not always means new process - event sending can be unified by Multiplicator
//   - On stop stream must close provided sub to notify it about end of sequence and free resources
type Stream[T any] interface {
	//  - Should subscribe provided subscriber to updates and return stop func
	//	- Subscriber must be closed anyway: on error (with CauseCanceled) or when process completed
	//	- Provided context is used ONLY to control startup time
	//  - context.CancelFunc should be equal to cluster.NilCancel if error occured
	Subscribe(startupCtx context.Context, sub Sub[T]) (stop context.CancelFunc, err error)
}

// Sub (subscriber) is a stateless listener of events
//
//   - There is no guarantee that every event will be delivered
//   - If send failed event can be omitted
//   - Every event must be immutable
type Sub[T any] interface {
	// 	- TrySend must never block
	//  - If event cannot be delivered, ErrSubBlocked should be returned
	// 	- If Sub is closed, ErrOutputClosed should be returned
	TrySend(event T) error
	// 	- Sub should be closed to free resources
	// 	- Close can block
	//  - Common close causes - CauseCanceled (=context.Canceled), CauseUnexpectedEOF (=io.ErrUnexpectedEOF) and CauseEOF (=io.EOF)
	//  - Only first Close call should take effect, others should be safe but useless
	Close(cause error)
	// Just a helper
	IsClosed() bool
}

// Create new Sub[T] with provided buffer size and channel-style handler
func NewSub[T any](bufferSize int) (Sub[T], Input[T]) {
	channel := make(chan T, bufferSize)
	cancellationCtx, cancel := context.WithCancel(context.Background())
	stopCallback := func(cause error) {
		if ParseCause(cause).IsCanceled() {
			cancel()
		}
	}
	sub := newChannelSub(channel, stopCallback)
	input := newChannelInput(channel, cancellationCtx, sub.GetStopCause)
	return sub, input
}

// Create a new Sub instance around old one with custom Close function and it's own Close guard
func CloneSub[T any](sub Sub[T], onClose func(cause error)) Sub[T] {
	return newClonedSub(sub, onClose)
}

// Describes some part of the stream pipeline
//
// Internal function must
//   - Produce new sub and link wrapped sub lifetime to it
//   - Close provided sub on error or after mapped sub close
type EventPipe[In, Out any] func(startupCtx context.Context, sub Sub[Out]) (Sub[In], error)

func (m EventPipe[In, Out]) MapStream(stream Stream[In]) Stream[Out] {
	return mappedStream[In, Out]{
		stream:  stream,
		mapping: m,
	}
}
func (m EventPipe[In, Out]) MapSub(startupCtx context.Context, sub Sub[Out]) (Sub[In], error) {
	mapped, err := m(startupCtx, sub)
	if err != nil {
		sub.Close(CauseCanceled) // optional, just to make sure that resources are deallocated
		return nil, err
	}
	return mapped, nil
}
func (p EventPipe[In, Out]) WithCheck(startupCheck func(context.Context) error) EventPipe[In, Out] {
	return func(startupCtx context.Context, sub Sub[Out]) (Sub[In], error) {
		if err := startupCheck(startupCtx); err != nil {
			sub.Close(CauseCanceled)
			return nil, err
		}
		return p(startupCtx, sub)
	}
}

type mappedStream[In, Out any] struct {
	stream  Stream[In]
	mapping EventPipe[In, Out]
}

func (p mappedStream[In, Out]) Subscribe(startupCtx context.Context, sub Sub[Out]) (context.CancelFunc, error) {
	mappedSub, err := p.mapping(startupCtx, sub)
	if err != nil {
		sub.Close(CauseCanceled) // optional, just to make sure that resources are deallocated
		return NilCancel, err
	}
	return p.stream.Subscribe(startupCtx, mappedSub)
}
