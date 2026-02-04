package clusterpipes

import (
	"context"

	"github.com/N1KlTA/go-cluster"
)

// provided function MUST be non-blocking
func MapEvents[In, Out any](mapping func(In) Out) cluster.EventPipe[In, Out] {
	actualMapping := func(data In) (Out, bool) {
		return mapping(data), true
	}
	return func(_ context.Context, sub cluster.Sub[Out]) (cluster.Sub[In], error) {
		return wrapSub(actualMapping, sub), nil
	}
}

// provided function MUST be non-blocking
func FilterEvents[T any](filter func(T) bool) cluster.EventPipe[T, T] {
	actualFilter := func(event T) (T, bool) {
		return event, filter(event)
	}
	return func(_ context.Context, sub cluster.Sub[T]) (cluster.Sub[T], error) {
		return wrapSub(actualFilter, sub), nil
	}
}

// provided function MUST be non-blocking
func ForEachEvent[T any](call func(T)) cluster.EventPipe[T, T] {
	actualCall := func(data T) (T, bool) {
		call(data)
		return data, true
	}
	return func(_ context.Context, sub cluster.Sub[T]) (cluster.Sub[T], error) {
		return wrapSub(actualCall, sub), nil
	}
}

// provided function MUST be non-blocking
func FilterMapEvents[In, Out any](mapping func(In) (Out, bool)) cluster.EventPipe[In, Out] {
	return func(_ context.Context, sub cluster.Sub[Out]) (cluster.Sub[In], error) {
		return wrapSub(mapping, sub), nil
	}
}

func wrapSub[In, Out any](f func(In) (Out, bool), sub cluster.Sub[Out]) cluster.Sub[In] {
	return wrappedSub[In, Out]{
		sub: sub,
		f:   f,
	}
}

type wrappedSub[In, Out any] struct {
	sub cluster.Sub[Out]
	f   func(In) (Out, bool)
}

func (c wrappedSub[In, Out]) TrySend(event In) error {
	res, ok := c.f(event)
	if !ok {
		return nil
	}
	return c.sub.TrySend(res)
}
func (c wrappedSub[In, Out]) Close(cause error) {
	c.sub.Close(cause)
}
func (c wrappedSub[In, Out]) IsClosed() bool {
	return c.sub.IsClosed()
}
