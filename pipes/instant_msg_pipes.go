package clusterpipes

import (
	"context"

	"github.com/N1KlTA/go-cluster"
)

// provided function MUST be non-blocking
func MapMsgs[In, Out any](mapping func(In) (Out, error)) cluster.MsgPipe[In, Out] {
	actualMapping := func(msg Msg[In]) (Msg[Out], bool) {
		return cluster.MsgMap(msg, mapping), true
	}
	return func(_ context.Context, consumer cluster.MsgConsumer[Out]) (cluster.MsgConsumer[In], error) {
		return wrapConsumer(actualMapping, consumer), nil
	}
}

// provided function MUST be non-blocking
func FilterMsgs[T any](filter func(T) (bool, error)) cluster.MsgPipe[T, T] {
	actualFilter := func(msg Msg[T]) (Msg[T], bool) {
		return msg.Filter(filter)
	}
	return func(_ context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		return wrapConsumer(actualFilter, consumer), nil
	}
}

// provided function MUST be non-blocking
func ForEachMsg[T any](call func(T) error) cluster.MsgPipe[T, T] {
	actualCall := func(msg Msg[T]) (Msg[T], bool) {
		return msg.Then(call), true
	}
	return func(_ context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		return wrapConsumer(actualCall, consumer), nil
	}
}

// provided function MUST be non-blocking
func FilterMapMsgs[In, Out any](mapping func(In) (cluster.Optional[Out], error)) cluster.MsgPipe[In, Out] {
	actualMapping := func(msg Msg[In]) (Msg[Out], bool) {
		return cluster.MsgFilterMap(msg, mapping)
	}
	return func(_ context.Context, consumer cluster.MsgConsumer[Out]) (cluster.MsgConsumer[In], error) {
		return wrapConsumer(actualMapping, consumer), nil
	}
}

func wrapConsumer[In, Out any](f func(In) (Out, bool), consumer cluster.CustomConsumer[Out]) cluster.CustomConsumer[In] {
	return wrappedConsumer[In, Out]{
		consumer: consumer,
		f:        f,
	}
}

type wrappedConsumer[In, Out any] struct {
	consumer cluster.CustomConsumer[Out]
	f        func(In) (Out, bool)
}

func (c wrappedConsumer[In, Out]) Send(ctx context.Context, msg In) error {
	res, ok := c.f(msg)
	if !ok {
		return nil
	}
	return c.consumer.Send(ctx, res)
}
func (c wrappedConsumer[In, Out]) Close(cause error) {
	c.consumer.Close(cause)
}
func (c wrappedConsumer[In, Out]) IsClosed() bool {
	return c.consumer.IsClosed()
}
