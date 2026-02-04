package clusterpipes

import (
	"context"
	"fmt"

	"github.com/N1KlTA/go-cluster"
)

// Sync pipes will process mapping for each incoming Message in a single thread
//
// As long, as single thread execution can be more optimised, it is a complicated pipeline,
// because commonly it should be used ONLY with batch calls via SyncBatchMsgs.
// If you are not ready to implement batch calls, look AsyncMsgMapping, it's much more naive and easy
//
// If you want to set timeout, you should modify function itself:
//
//	func(ctx context.Context, data In) (Out, error) {
//		ctx, cancel := context.WithTimeout(ctx)
//		defer cancel()
//		return someOtherFunc(ctx, data) // your logic
//	}

// Sync pipe will process operation for each incoming Message in a single thread
//
// WARN: This pipe can SLOW DOWN your execution
//
//   - It is expected to use it ONLY with batching, otherwise you will got problems
//   - If you are not ready to implement batch calls, than PLEASE look at Async pipes - they're much easier
func SyncMapMsgs[In, Out any](mapping func(context.Context, In) (Out, error)) cluster.MsgPipe[In, Out] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[Out]) (cluster.MsgConsumer[In], error) {
		res, input := cluster.NewMsgConsumer[In]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			for msg := range input.Range() {
				newMsg := cluster.MsgMap(msg, func(data In) (Out, error) {
					return mapping(input.Context(), data)
				})
				if err := consumer.Send(input.Context(), newMsg); err != nil {
					if input.Context().Err() == nil {
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

// Sync pipe will process operation for each incoming Message in a single thread
//
// WARN: This pipe can SLOW DOWN your execution
//
//   - It is expected to use it ONLY with batching, otherwise you will got problems
//   - If you are not ready to implement batch calls, than PLEASE look at Async pipes - they're much easier
func SyncFilterMapMsgs[In, Out any](mapping func(context.Context, In) (cluster.Optional[Out], error)) cluster.MsgPipe[In, Out] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[Out]) (cluster.MsgConsumer[In], error) {
		res, input := cluster.NewMsgConsumer[In]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			for msg := range input.Range() {
				newMsg, ok := cluster.MsgFilterMap(msg, func(data In) (cluster.Optional[Out], error) {
					return mapping(input.Context(), data)
				})
				if !ok {
					continue
				}
				if err := consumer.Send(input.Context(), newMsg); err != nil {
					if input.Context().Err() == nil {
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

// Sync pipe will process operation for each incoming Message in a single thread
//
// WARN: This pipe can SLOW DOWN your execution
//
//   - It is expected to use it ONLY with batching, otherwise you will got problems
//   - If you are not ready to implement batch calls, than PLEASE look at Async pipes - they're much easier
func SyncFilterMsgs[T any](filter func(context.Context, T) (bool, error)) cluster.MsgPipe[T, T] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		res, input := cluster.NewMsgConsumer[T]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			for msg := range input.Range() {
				newMsg, ok := msg.Filter(func(data T) (bool, error) {
					return filter(input.Context(), data)
				})
				if !ok {
					continue
				}
				if err := consumer.Send(input.Context(), newMsg); err != nil {
					if input.Context().Err() == nil {
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

// Sync pipe will process operation for each incoming Message in a single thread
//
// WARN: This pipe can SLOW DOWN your execution
//
//   - It is expected to use it ONLY with batching, otherwise you will got problems
//   - If you are not ready to implement batch calls, than PLEASE look at Async pipes - they're much easier
func SyncForEachMsg[T any](call func(context.Context, T) error) cluster.MsgPipe[T, T] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		res, input := cluster.NewMsgConsumer[T]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			for msg := range input.Range() {
				newMsg := msg.Then(func(data T) error {
					return call(input.Context(), data)
				})
				if err := consumer.Send(input.Context(), newMsg); err != nil {
					if input.Context().Err() == nil {
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
