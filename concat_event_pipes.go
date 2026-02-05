package cluster

import "context"

// Go has awful generics and horrible "..." syntax
// I hate runtime type checks, so...
// There is no other solution

func Concat2EventPipes[In, Mid, Out any](first EventPipe[In, Mid], second EventPipe[Mid, Out]) EventPipe[In, Out] {
	return func(startupCtx context.Context, sub Sub[Out]) (Sub[In], error) {
		midSub, err := second.MapSub(startupCtx, sub)
		if err != nil {
			return nil, err
		}
		return first.MapSub(startupCtx, midSub)
	}
}
func Concat3EventPipes[T1, T2, T3, T4 any](p1 EventPipe[T1, T2], p2 EventPipe[T2, T3], p3 EventPipe[T3, T4]) EventPipe[T1, T4] {
	return func(startupCtx context.Context, sub Sub[T4]) (Sub[T1], error) {
		p3in, err := p3.MapSub(startupCtx, sub)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapSub(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapSub(startupCtx, p2in)
	}
}
func Concat4EventPipes[T1, T2, T3, T4, T5 any](p1 EventPipe[T1, T2], p2 EventPipe[T2, T3], p3 EventPipe[T3, T4], p4 EventPipe[T4, T5]) EventPipe[T1, T5] {
	return func(startupCtx context.Context, sub Sub[T5]) (Sub[T1], error) {
		p4in, err := p4.MapSub(startupCtx, sub)
		if err != nil {
			return nil, err
		}
		p3in, err := p3.MapSub(startupCtx, p4in)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapSub(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapSub(startupCtx, p2in)
	}
}
func Concat5EventPipes[T1, T2, T3, T4, T5, T6 any](p1 EventPipe[T1, T2], p2 EventPipe[T2, T3], p3 EventPipe[T3, T4], p4 EventPipe[T4, T5], p5 EventPipe[T5, T6]) EventPipe[T1, T6] {
	return func(startupCtx context.Context, sub Sub[T6]) (Sub[T1], error) {
		p5in, err := p5.MapSub(startupCtx, sub)
		if err != nil {
			return nil, err
		}
		p4in, err := p4.MapSub(startupCtx, p5in)
		if err != nil {
			return nil, err
		}
		p3in, err := p3.MapSub(startupCtx, p4in)
		if err != nil {
			return nil, err
		}
		p2in, err := p2.MapSub(startupCtx, p3in)
		if err != nil {
			return nil, err
		}
		return p1.MapSub(startupCtx, p2in)
	}
}
