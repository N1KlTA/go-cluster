package clusterpipes

import (
	"context"

	"github.com/N1KlTA/go-cluster"
)

// Broken pipes can be returned instead of errors in some cases,
// also they can be used for testing

// On any usage try will return a provided error
func BrokenMsgPipe[In, Out any](err error) cluster.MsgPipe[In, Out] {
	return func(_ context.Context, consumer cluster.MsgConsumer[Out]) (cluster.MsgConsumer[In], error) {
		consumer.Close(cluster.CauseCanceled)
		return nil, err
	}
}

// On any usage try will return a provided error
func BrokenEventPipe[In, Out any](err error) cluster.EventPipe[In, Out] {
	return func(_ context.Context, sub cluster.Sub[Out]) (cluster.Sub[In], error) {
		sub.Close(cluster.CauseCanceled)
		return nil, err
	}
}
