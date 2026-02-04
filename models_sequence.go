package cluster

import "context"

// This file contains basic interfaces for working with message sequences (guarantee at least once)

// CustomProducer is a message emitter
//
//   - On startup producer must get current state and emmit every single message from it
//   - On stop producer must close provided consumer to notify it about end of sequence and free resources
//   - CustomProducer should wait every time connected consumer isn't ready for consuming
//   - If skipping is inevitable, producer should close receiver chan instead and finish process
//   - State of producer can be controlled differently, based on a task and outer service
//   - It's okay to limit number of active session to one
//   - Remember - every producer session is always a new process
type CustomProducer[T any] interface {
	//   - Should start producing events to consumer and return stop function
	//	 - Consumer must be closed anyway: on error (with CauseCanceled) or after process completed
	// 	 - Provided context is used ONLY to control startup time
	//   - context.CancelFunc should be equal to cluster.NilCancel if error occured
	Start(startupCtx context.Context, consumer CustomConsumer[T]) (context.CancelFunc, error)
}

// CustomConsumer is a statefull listener of messages
//
//   - Every message should be delivered to consumer until producing stopped
//   - Messages should be immutable
type CustomConsumer[T any] interface {
	// 	- Send should block until message is sended or context is canceled
	// 	- If Consumer is closed, ErrOutputClosed should be returned
	Send(ctx context.Context, message T) error
	// 	- Close must be called to make resources free
	// 	- Close can block
	//  - Only first Close call should take effect, others should be safe but useless
	Close(cause error)
	// Just a helper
	IsClosed() bool
}

// Useful shortcuts
type (
	MsgProducer[T any] = CustomProducer[Msg[T]]
	MsgConsumer[T any] = CustomConsumer[Msg[T]]
)

// Message Pipe is the library standard for processing messages
//
//   - Pipe always must respect indexation (Headers) of processed messages
//   - Output indexation MUST match to input indexation
//   - To properly handle header mutations (in case of batching/splitting messages) check Header description
//
// You can find a lot of standard pipes inside of clusterpipes package
type MsgPipe[In, Out any] func(startupCtx context.Context, consumer MsgConsumer[Out]) (MsgConsumer[In], error)

func (p MsgPipe[In, Out]) MapProducer(producer CustomProducer[Msg[In]]) CustomProducer[Msg[Out]] {
	return mappedProducer[Msg[In], Msg[Out]]{
		producer: producer,
		mapping:  p,
	}
}
func (p MsgPipe[In, Out]) MapConsumer(ctx context.Context, consumer CustomConsumer[Msg[Out]]) (CustomConsumer[Msg[In]], error) {
	mapped, err := p(ctx, consumer)
	if err != nil {
		consumer.Close(CauseCanceled) // optional, just to make sure that resources are deallocated
		return nil, err
	}
	return mapped, nil
}
func (p MsgPipe[In, Out]) WithCheck(startupCheck func(context.Context) error) MsgPipe[In, Out] {
	return func(startupCtx context.Context, consumer MsgConsumer[Out]) (MsgConsumer[In], error) {
		if err := startupCheck(startupCtx); err != nil {
			consumer.Close(CauseCanceled)
			return nil, err
		}
		return p(startupCtx, consumer)
	}
}

type mappedProducer[In, Out any] struct {
	producer CustomProducer[In]
	mapping  func(context.Context, CustomConsumer[Out]) (CustomConsumer[In], error)
}

func (p mappedProducer[In, Out]) Start(startupCtx context.Context, consumer CustomConsumer[Out]) (context.CancelFunc, error) {
	mapped, err := p.mapping(startupCtx, consumer)
	if err != nil {
		consumer.Close(CauseCanceled) // optional, just to make sure that resources are deallocated
		return NilCancel, err
	}
	return p.producer.Start(startupCtx, mapped)
}

// Create new unbuffered Consumer[T] with channel-style handler
func NewMsgConsumer[T any]() (MsgConsumer[T], Input[Msg[T]]) {
	return NewMsgConsumerWithBuffer[T](0)
}

// Create new MsgConsumer[T] with buffer
//
// In some (very rare) cases you can be interested in buffered consumer.
// Actually i know only one very specific usecase (clusterpipes.AsyncExecuteMsgs).
// If you aren't sure do you need buffer or not - always prefer NewMsgConsumer instead
// This function can be deprecated in future
func NewMsgConsumerWithBuffer[T any](bufferSize int) (MsgConsumer[T], Input[Msg[T]]) {
	// Consumer shouldn't have any buffer to make delivery more clear for producer
	channel := make(chan Msg[T])
	// Actually I already implemented my own context.Context (even with context.afterFuncer implementation for faster child canceling)
	// It was connected to the channel lifetime and designed to beautifully reuse consumerChannel.closeSignal as Done(), but...
	// Unfortunately, we don't want to cancel context.Context on stop cause EOF or UnexpectedEOF (when we want graceful stop)
	// And it isn't *some assumption*, no, i have tested my previous version and it wasn't intuitive at all
	// So yeah, now I use standard context.Context, yes, it's an overhead but it's much more comfortable to use
	// And yes, I am still sad about deleting beautiful custom context.Context implementation... :(
	cancellationCtx, cancel := context.WithCancel(context.Background())
	callback := func(cause error) {
		if ParseCause(cause).IsCanceled() {
			cancel()
		}
	}
	consumer := newChannelConsumer(channel, callback) // will handle channel close
	input := newChannelInput(channel, cancellationCtx, consumer.GetStopCause)
	return consumer, input
}
