package clusterpipes

import (
	"context"
	"fmt"

	"github.com/N1KlTA/go-cluster"
)

func StopOnMsgError[T any]() cluster.MsgPipe[T, T] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		return wrapConsumer(func(m cluster.Msg[T]) (cluster.Msg[T], bool) {
			if m.Err() != nil {
				consumer.Close(fmt.Errorf("message failed: %s", m.Err())) // as cluster.CauseUnexpectedEOF
			}
			return m, true // will return ErrOutputClosed to sender if we closed it before
		}, consumer), nil
	}
}

func HandleMsgError[T any](handleError func(msgErr error) (remain bool)) cluster.MsgPipe[T, T] {
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		return wrapConsumer(func(m cluster.Msg[T]) (cluster.Msg[T], bool) {
			if msgErr := m.Err(); msgErr != nil {
				remain := handleError(msgErr)
				return m, remain
			}
			return m, true
		}, consumer), nil
	}
}
