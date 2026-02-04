package clusterpipes

import (
	"context"
	"fmt"

	"github.com/N1KlTA/go-cluster"
)

func SplitMsgs[T any]() cluster.MsgPipe[[]T, T] {
	return func(_ context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[[]T], error) {
		res, input := cluster.NewMsgConsumer[[]T]()
		go func() {
			defer func() {
				consumer.Close(input.GetStopCause())
			}()
			for msg := range input.Range() {
				for _, newMsg := range splitMsg(msg) {
					if err := consumer.Send(input.Context(), newMsg); err != nil {
						if input.Context().Err() == nil { // unexpected send error
							// we don't want to wrap it to avoid stop cause misinterpretation
							consumer.Close(fmt.Errorf("send failed unexpectedly: %s", err.Error()))
						}
						return
					}
				}
			}
		}()
		return res, nil
	}
}

func splitMsg[T any](msg cluster.Msg[[]T]) []cluster.Msg[T] {
	content, err := msg.GetContent()
	if err != nil {
		return []cluster.Msg[T]{{
			Header: msg.GetHeader(),
			Error:  err,
		}}
	}
	if len(content) == 0 {
		return []cluster.Msg[T]{}
	}
	res := make([]cluster.Msg[T], 0, len(content))
	// header must be immutable so it's okay to use one header on multiple messages
	partialHeader := &cluster.Header{
		NewState: msg.Header.Source.Start,

		Source: cluster.HeaderSource{
			Start: msg.Header.Source.Start,
			End:   msg.Header.Source.End,
		},

		SequenceId: msg.Header.SequenceId,
	}
	for _, el := range content[:len(content)-1] {
		res = append(res, cluster.Msg[T]{
			Header:  partialHeader,
			Content: el,
		})
	}
	res = append(res, cluster.Msg[T]{
		Header:  msg.Header,
		Content: content[len(content)-1],
	})
	return res
}
