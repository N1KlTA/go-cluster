package clusterpipes

import (
	"context"
	"fmt"

	"github.com/N1KlTA/go-cluster"
)

func AsyncPrepareMsgs[T any]() cluster.MsgPipe[T, Task[T]] {
	return MapMsgs(func(input T) (Task[T], error) {
		return NewTask(input), nil
	})
}

func AsyncMapMsgs[In, Out any](mapping func(context.Context, In) (Out, error)) cluster.MsgPipe[Task[In], Task[Out]] {
	return MapMsgs(func(t Task[In]) (Task[Out], error) {
		return MapTask(t, mapping), nil
	})
}

func AsyncFilterMapMsgs[In, Out any](mapping func(context.Context, In) (cluster.Optional[Out], error)) cluster.MsgPipe[Task[In], Task[Out]] {
	return MapMsgs(func(t Task[In]) (Task[Out], error) {
		return FilterMapTask(t, mapping), nil
	})
}

func AsyncFilterMsgs[T any](filter func(context.Context, T) (bool, error)) cluster.MsgPipe[Task[T], Task[T]] {
	return MapMsgs(func(t Task[T]) (Task[T], error) {
		return t.Filter(filter), nil
	})
}

func AsyncForEachMsg[T any](call func(context.Context, T) error) cluster.MsgPipe[Task[T], Task[T]] {
	return MapMsgs(func(t Task[T]) (Task[T], error) {
		return MapTask(t, func(ctx context.Context, data T) (nilRes T, _ error) {
			if err := call(ctx, data); err != nil {
				return nilRes, err
			}
			return data, nil
		}), nil
	})
}

// It is a scratch realization and it's bad, i will rewrite it in future
//
// The problem is:
//   - Imagine that we try to send a new task, it's being converted into promise with detaching
//   - But then sending context exceeded before task being successfully pushed on promise channel
//   - Now we have a useless, uncounted goroutine outside
//
// It isn't so critical but it is bad, i know about it and will fix it soon
func AsyncExecuteMsgs[T any](config AsyncExecutorConfig) cluster.MsgPipe[Task[T], T] {
	return func(startupCtx context.Context, consumer cluster.CustomConsumer[cluster.Msg[T]]) (cluster.CustomConsumer[cluster.Msg[Task[T]]], error) {
		// pretty unique case when buffer can actually be useful
		promiseConsumer, input := cluster.NewMsgConsumerWithBuffer[Promise[cluster.Optional[T]]](config.getExecutionBuffer())

		executionCtx, stopExecution := context.WithCancel(input.Context())

		// Pipeline to start detached execution:
		res, err := MapMsgs(func(task Task[T]) (Promise[cluster.Optional[T]], error) {
			if timeout, ok := config.getTimeout(); ok {
				task = task.WithTimeout(timeout)
			}
			return task.Detach(executionCtx), nil
		}).MapConsumer(startupCtx, promiseConsumer)
		if err != nil { // in practice impossible but better to check
			stopExecution()
			consumer.Close(cluster.CauseCanceled)
			return nil, err
		}

		go func() {
			defer stopExecution()
			defer func() {
				consumer.Close(input.GetStopCause())
			}()

			for msg := range input.Range() {
				newMsg, ok := cluster.MsgFilterMap(msg, func(promise Promise[cluster.Optional[T]]) (cluster.Optional[T], error) {
					// execution context was derived from input.Context(), so we can wait in blocking mode
					return promise.Get()
				})
				if !ok {
					continue
				}
				if err := consumer.Send(input.Context(), newMsg); err != nil {
					if input.Context().Err() == nil { // unexpected send error
						// we don't want to wrap it to avoid stop cause misinterpretation
						consumer.Close(fmt.Errorf("send failed unexpectedly: %s", err.Error()))
					}
					return
				}
			}
		}()
		return res, nil
	}
}
