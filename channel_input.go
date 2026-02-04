package cluster

import (
	"context"
	"iter"
)

func newChannelInput[T any](
	channel <-chan T,
	cancellationCtx context.Context,
	getStopCause func() error,
) channelInput[T] {
	return channelInput[T]{
		cancellationCtx: cancellationCtx,
		channel:         channel,
		getStopCause:    getStopCause,
	}
}

// static type check
var _ Input[int] = &channelInput[int]{}

type channelInput[T any] struct {
	cancellationCtx context.Context
	channel         <-chan T
	getStopCause    func() error
}

func (i channelInput[T]) Context() context.Context {
	return i.cancellationCtx
}
func (i channelInput[T]) Channel() <-chan T {
	return i.channel
}
func (i channelInput[T]) Recv(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var nilT T
		return nilT, ctx.Err()
	case data, ok := <-i.channel:
		if !ok {
			return data, i.getStopCause()
		}
		return data, nil
	case <-i.cancellationCtx.Done(): // canceled
		var nilT T
		return nilT, i.getStopCause()
	}
}
func (i channelInput[T]) Range() iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			msg, err := i.Recv(context.Background())
			if err != nil {
				return
			}
			if !yield(msg) {
				return
			}
		}
	}
}
func (i channelInput[T]) GetStopCause() error {
	return i.getStopCause()
}
