package clusterpipes

import (
	"github.com/N1KlTA/go-cluster"
)

// "To empty" pipes can be useful if you want to drop data after processing
//
// As an example - there can be a trigger-pipe (for each) on specific events and you want only to know
// is it alive or not. In this case "to empty" will make you listening to an empty channel until any stop signal will arrive
// Otherwise you will need to actually read events from stream which is useless and can be annoing
//
// Other example - accepting pipes with signature cluster.MsgPipe[T, struct{}]. You don't want to know which transformations are used inside,
// you just want to know about it success. Using cluster.MsgPipe[T, T] will push caller to make a recover,
// which probably will be useless but expensive. So signature cluster.MsgPipe[T, struct{}] can be the best decision for functions accepting pipelines

// Will remove content from messages
//
// Emtpy messages can be left for Commit purposes by specifying dropSuccessful=false
func ToEmptyMsg[T any](dropSuccessful bool) cluster.MsgPipe[T, struct{}] {
	return FilterMapMsgs(func(t T) (cluster.Optional[struct{}], error) {
		return cluster.AsOptional(struct{}{}, !dropSuccessful), nil
	})
}

// Will remove all events from the stream
func ToEmptyEvent[T any]() cluster.EventPipe[T, struct{}] {
	return FilterMapEvents(func(t T) (struct{}, bool) {
		return struct{}{}, false
	})
}
