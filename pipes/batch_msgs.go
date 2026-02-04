package clusterpipes

import (
	"context"
	"fmt"
	"time"

	"github.com/N1KlTA/go-cluster"
)

type BatchingConfig struct {
	// Indicates how often batcher will try to forward batched successful messages
	//
	// - Err messages aren't batched so limit will not affect them
	// - Default = 200 * time.Millisecond
	SendTimedelta time.Duration `yaml:"sendTimedelta"`
	// Batch size limit
	//
	// - Default = 40
	// - You can use -1 to make it unlimited, but it can be dangerous if upstream producer can generate too much messages
	// - Any Err message will anyway be counted as end of the batch
	MaxBatchSize int `yaml:"maxBatchSize"`
}

var (
	batchingDefaultSendTimedelta = 200 * time.Millisecond
	batchingDefaultMaxBatchSize  = 40
	batchingDefaultMaxFailCount  = 5

	batchingUnlimitedBatchSize = -1
)

func (c *BatchingConfig) getSendTimedelta() time.Duration {
	if c.SendTimedelta == 0 {
		return batchingDefaultSendTimedelta
	}
	return c.SendTimedelta
}
func (c *BatchingConfig) isCacheFull(curSize int) bool {
	if c.MaxBatchSize == batchingUnlimitedBatchSize {
		return false
	}
	if c.MaxBatchSize == 0 {
		return curSize >= batchingDefaultMaxBatchSize
	}
	return curSize >= c.MaxBatchSize
}

// This is scratch implementation, i will rewrite it later
//
// TODO: rewrite
func BatchMsgs[T any](config *BatchingConfig) cluster.MsgPipe[T, []T] {
	return func(_ context.Context, consumer cluster.MsgConsumer[[]T]) (cluster.MsgConsumer[T], error) {
		res, input := cluster.NewMsgConsumer[T]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			sendTicker := time.NewTicker(config.getSendTimedelta())
			defer sendTicker.Stop()
			finished := false
			for !finished {
				sendReady := false
				nonErrCache := make([]Msg[T], 0) // TODO: add initial buffer?
				errMsg := cluster.None[Msg[T]]() // this message should be commited AFTER cache (if it exists)
				cacheReady := false
				for !cacheReady {
					select {
					case <-input.Context().Done():
						return
					case msg, ok := <-input.Channel():
						if !ok {
							if len(nonErrCache) == 0 {
								return
							}
							cacheReady = true
							finished = true
							break
						}
						if msg.Error != nil {
							errMsg = cluster.Some(msg)
							cacheReady = true
							break
						}
						nonErrCache = append(nonErrCache, msg)
						if sendReady || config.isCacheFull(len(nonErrCache)) {
							cacheReady = true
						}
					case <-sendTicker.C:
						sendReady = true
						if len(nonErrCache) > 0 {
							cacheReady = true
						}
					}
				}
				if len(nonErrCache) != 0 {
					if !sendReady {
						select {
						case <-input.Context().Done():
							return
						case <-sendTicker.C:
							sendReady = true
						}
					}
					newMsg := batchNonErrMsgs(nonErrCache)
					if err := consumer.Send(input.Context(), newMsg); err != nil {
						if input.Context().Err() == nil { // unexpected send error
							// we don't want to wrap it to avoid stop cause misinterpretation
							consumer.Close(fmt.Errorf("send failed unexpectedly: %s", err.Error()))
						}
						return
					}
					sendReady = false
				}
				newErrMsg, ok := cluster.OptionalMap(errMsg, func(errMsg Msg[T]) Msg[[]T] {
					return Msg[[]T]{
						Header: errMsg.Header,
						Error:  errMsg.Error,
					}
				}).Unwrap()
				if !ok {
					continue
				}
				if err := consumer.Send(input.Context(), newErrMsg); err != nil {
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

func batchNonErrMsgs[T any](nonErrMsg []Msg[T]) Msg[[]T] {
	newHeader := &cluster.Header{
		NewState: nonErrMsg[len(nonErrMsg)-1].Header.NewState,

		Source: cluster.HeaderSource{
			Start: nonErrMsg[0].Header.Source.Start,
			End:   nonErrMsg[len(nonErrMsg)-1].Header.Source.End,
		},

		SequenceId: nonErrMsg[0].Header.SequenceId,
	}
	newContent := make([]T, 0, len(nonErrMsg))
	for _, msg := range nonErrMsg {
		newContent = append(newContent, msg.Content)
	}
	return Msg[[]T]{
		Header:  newHeader,
		Content: newContent,
	}
}
