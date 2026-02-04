package cluster

import "context"

// Go has awful generics and horrible "..." syntax
// I hate runtime type checks, so...
// There is no other solution

func Concat2MsgPipes[In, Mid, Out any](first MsgPipe[In, Mid], second MsgPipe[Mid, Out]) MsgPipe[In, Out] {
	return func(startupCtx context.Context, consumer MsgConsumer[Out]) (MsgConsumer[In], error) {
		midConsumer, err := second.MapConsumer(startupCtx, consumer)
		if err != nil {
			return nil, err
		}
		return first.MapConsumer(startupCtx, midConsumer)
	}
}
func Concat3MsgPipes[T1, T2, T3, T4 any](p1 MsgPipe[T1, T2], p2 MsgPipe[T2, T3], p3 MsgPipe[T3, T4]) MsgPipe[T1, T4] {
	return func(startupCtx context.Context, consumer MsgConsumer[T4]) (MsgConsumer[T1], error) {
		p3in, err := p3.MapConsumer(startupCtx, consumer)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapConsumer(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapConsumer(startupCtx, p2in)
	}
}
func Concat4MsgPipes[T1, T2, T3, T4, T5 any](p1 MsgPipe[T1, T2], p2 MsgPipe[T2, T3], p3 MsgPipe[T3, T4], p4 MsgPipe[T4, T5]) MsgPipe[T1, T5] {
	return func(startupCtx context.Context, consumer MsgConsumer[T5]) (MsgConsumer[T1], error) {
		p4in, err := p4.MapConsumer(startupCtx, consumer)
		if err != nil {
			return nil, err
		}
		p3in, err := p3.MapConsumer(startupCtx, p4in)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapConsumer(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapConsumer(startupCtx, p2in)
	}
}
func Concat5MsgPipes[T1, T2, T3, T4, T5, T6 any](p1 MsgPipe[T1, T2], p2 MsgPipe[T2, T3], p3 MsgPipe[T3, T4], p4 MsgPipe[T4, T5], p5 MsgPipe[T5, T6]) MsgPipe[T1, T6] {
	return func(startupCtx context.Context, consumer MsgConsumer[T6]) (MsgConsumer[T1], error) {
		p5in, err := p5.MapConsumer(startupCtx, consumer)
		if err != nil {
			return nil, err
		}
		p4in, err := p4.MapConsumer(startupCtx, p5in)
		if err != nil {
			return nil, err
		}
		p3in, err := p3.MapConsumer(startupCtx, p4in)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapConsumer(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapConsumer(startupCtx, p2in)
	}
}
