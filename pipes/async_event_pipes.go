package clusterpipes

import (
	"context"

	"github.com/N1KlTA/go-cluster"
)

func AsyncPrepareEvents[T any]() cluster.EventPipe[T, Task[T]] {
	return MapEvents(func(input T) Task[T] {
		return NewTask(input)
	})
}

func AsyncMapEvents[In, Out any](mapping func(context.Context, In) (res Out, ok bool)) cluster.EventPipe[Task[In], Task[Out]] {
	return MapEvents(func(t Task[In]) Task[Out] {
		return FilterMapTask(t, func(ctx context.Context, data In) (cluster.Optional[Out], error) {
			return cluster.AsOptional(mapping(ctx, data)), nil
		})
	})
}

func AsyncFilterEvents[T any](filter func(context.Context, T) (ok bool)) cluster.EventPipe[Task[T], Task[T]] {
	return MapEvents(func(t Task[T]) Task[T] {
		return t.Filter(func(ctx context.Context, t T) (bool, error) {
			return filter(ctx, t), nil
		})
	})
}

func AsyncFilterMapEvents[In, Out any](mapping func(context.Context, In) (res cluster.Optional[Out], ok bool)) cluster.EventPipe[Task[In], Task[Out]] {
	return MapEvents(func(t Task[In]) Task[Out] {
		return FilterMapTask(t, func(ctx context.Context, data In) (cluster.Optional[Out], error) {
			res, ok := mapping(ctx, data)
			if !ok {
				return cluster.None[Out](), nil
			}
			return res, nil
		})
	})
}

func AsyncForEachEvent[T any](call func(context.Context, T) (ok bool)) cluster.EventPipe[Task[T], Task[T]] {
	return MapEvents(func(t Task[T]) Task[T] {
		return FilterMapTask(t, func(ctx context.Context, data T) (cluster.Optional[T], error) {
			if ok := call(ctx, data); !ok {
				return cluster.None[T](), nil
			}
			return cluster.Some(data), nil
		})
	})
}

// It is a scratch realization and it's bad, i will rewrite it in future
//
// The problem is:
//   - Imagine that we try to send a new task, it's being converted into promise with detaching
//   - But then sending context exceeded before task being successfully pushed on promise channel
//   - Now we have a useless, uncounted goroutine outside
//
// It isn't so critical (pretty rare case) but it is bad, i know about it and will fix it soon
func AsyncExecuteEvents[T any](config AsyncExecutorConfig) cluster.EventPipe[Task[T], T] {
	return func(startupCtx context.Context, sub cluster.Sub[T]) (cluster.Sub[Task[T]], error) {
		promiseSub, input := cluster.NewSub[Promise[cluster.Optional[T]]](config.getExecutionBuffer())

		executionCtx, stopExecution := context.WithCancel(input.Context())

		// Pipeline to start detached execution:
		res, err := MapEvents(func(task Task[T]) Promise[cluster.Optional[T]] {
			if timeout, ok := config.getTimeout(); ok {
				task = task.WithTimeout(timeout)
			}
			return task.Detach(executionCtx)
		}).MapSub(startupCtx, promiseSub)
		if err != nil { // in practice impossible but better to check
			stopExecution()
			sub.Close(cluster.CauseCanceled)
			return nil, err
		}

		go func() {
			defer stopExecution()
			defer func() {
				sub.Close(input.GetStopCause())
			}()

			for promise := range input.Range() {
				resOpt, err := promise.Get()
				res, ok := resOpt.Unwrap()
				// actually error is pretty unexpected here - we work with event mappings that cannot produce errors
				if err != nil || !ok {
					continue
				}
				sub.TrySend(res)
			}
		}()
		return res, nil
	}
}
