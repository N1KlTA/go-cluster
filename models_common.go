package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
)

// Input interface, which implementation you will get via creating standard Consumers/Subs
type Input[T any] interface {
	// This context is designed for process-linked operations (like further sends)
	//
	// Context will be canceled ONLY if the upcoming producer/stream send StopCauseCanceled,
	// that's needed for an immediate stop of further processes, otherwise you should listen messages/events
	// until the end of the Channel (you can retrieve actual StopCause via GetStopCause method)
	Context() context.Context
	// Input channel for the updates, as long as it's more preferable to use Recv/Range methods, this variant also can be used
	// It's recommended to use it with select-case syntax:
	//
	//	select {
	//		case <-input.Context().Done():
	//			// immediate stop
	//		case newUpdate, ok := input.Channel():
	//			if !ok {
	//				// if you want to understand stop cause type, you can use helper:
	//				parsedCause := cluster.ParseCause(input.GetStopCause())
	//				// handle stop
	//				break
	//			}
	//			// process newUpdate
	//		case <-ticker.C: // some custom cases
	//			// some custom logic
	//	}
	//
	// If you don't need to wait for other cases, you probably can describe you logic only with Recv or Range methods
	Channel() <-chan T
	// Return provided from upcoming process stop cause
	// To parse it, you can use cluster.ParseCause
	// You can finds Causes and their descriptions below
	GetStopCause() error
	// Recv will block until any of 4 events:
	//	- Provided ctx canceled
	//	- Channel closed with CauseCanceled
	//	- New message arrived
	//	- Channel closed with other cause and there is no pending messages left
	Recv(ctx context.Context) (T, error)
	// Range is a better interface in case of for loops - it will cancel loop immediately if upcoming process sends CauseCanceled
	// To use it just write this:
	//
	//	for update := range input.Range() {
	//		// handle update
	//	}
	//	// handle stop
	//
	// This description needed in case you never seen custom range implementations before - they are actually pretty rare
	Range() iter.Seq[T]
}

// Common stop causes
//
// There are three main close causes
//   - CauseUnexpectedEOF (io.ErrUnexpectedEOF):
//     Should stop gracefully and restart if needed
//   - CauseEOF (io.EOF):
//     Should stop gracefully, no need to restart - all data sended
//   - CauseCanceled (context.Canceled):
//     Should stop immediately, restart is optional
//
// Any nil or custom cause should be treated as CauseUnexpectedEOF - it's a default cause
// Recommended way of parsing - via using cluster.ParseCause function
var (
	CauseUnexpectedEOF = io.ErrUnexpectedEOF // should stop gracefully and restart if needed
	CauseEOF           = io.EOF              // should stop gracefully, no need to restart - all data sended
	CauseCanceled      = context.Canceled    // should stop immediately, restart is optional
)

// Will return a enum to analize close cause
func ParseCause(cause error) CauseClass {
	if errors.Is(cause, CauseCanceled) {
		return CauseClassCanceled
	}
	if errors.Is(cause, CauseEOF) {
		return CauseClassEOF
	}
	return CauseClassUnexpectedEOF // any custom cause (including nil) will be treatead as UnexpectedEOF
}

// Helper ENUM to parse Cause
type CauseClass int

// Helper ENUM values which can help you parsing actual cause
//
// There are three close causes classes:
//   - CauseClassUnexpectedEOF:
//     Should stop gracefully and restart if needed
//   - CauseClassEOF:
//     Should stop gracefully, no need to restart - all data sended
//   - CauseClassCanceled:
//     Should stop immediately, restart is optional
const (
	CauseClassUnexpectedEOF CauseClass = iota
	CauseClassEOF
	CauseClassCanceled
)

// Will return a basic version of the class error
func (c CauseClass) Err() error {
	switch c {
	case CauseClassUnexpectedEOF:
		return CauseUnexpectedEOF
	case CauseClassEOF:
		return CauseEOF
	case CauseClassCanceled:
		return CauseCanceled
	default:
		panic(c)
	}
}
func (c CauseClass) IsUnexpectedEOF() bool {
	return c == CauseClassUnexpectedEOF
}
func (c CauseClass) IsEOF() bool {
	return c == CauseClassEOF
}
func (c CauseClass) IsCanceled() bool {
	return c == CauseClassCanceled
}

// Common sending errors
var (
	ErrOutputClosed  = errors.New("output already closed")
	ErrSubOverloaded = errors.New("sub currently overloaded")
)

// Nil cancel is a reusable cancel which is recommended to return on error instead of nil context.CancelFunc to make it safier
//
// As long as stop must be useless on error, we want to avoid any panic-triggers
func NilCancel() {}

// CustomMsg is a common struct for messages, provided by a producer
//
//   - Header should be designed to carry some additional info about message (like id, offset, e.t.c)
//   - Any transformation of the Message content should take care about header
//
// As long, as you can use CustomMsg, it's highly recommended to use standard Msg (it comes with standard Header)
// Main reason - standard Header type support batching and splitting, that can be very useful in some cases
// Maybe CustomMsg will be deprecated in future
type CustomMsg[Header, Content any] struct {
	Header  Header
	Content Content
	Error   error
}

func (m CustomMsg[Header, Content]) GetHeader() Header {
	return m.Header
}
func (m CustomMsg[Header, Content]) GetContent() (Content, error) {
	return m.Content, m.Error
}
func (m CustomMsg[Header, Content]) Err() error {
	return m.Error
}
func (m CustomMsg[Header, Content]) Map(f func(Content) (Content, error)) CustomMsg[Header, Content] {
	return MsgMap(m, f)
}
func (m CustomMsg[Header, Content]) FilterMap(f func(Content) (Optional[Content], error)) (CustomMsg[Header, Content], bool) {
	return MsgFilterMap(m, f)
}
func (m CustomMsg[Header, Content]) Filter(f func(Content) (bool, error)) (CustomMsg[Header, Content], bool) {
	if m.Error != nil {
		return m, true
	}
	remain, err := f(m.Content)
	if err != nil {
		return CustomMsg[Header, Content]{
			Header: m.Header,
			Error:  err,
		}, true
	}
	return m, remain
}
func (m CustomMsg[Header, Content]) Then(f func(Content) error) CustomMsg[Header, Content] {
	if m.Error != nil {
		return m
	}
	if err := f(m.Content); err != nil {
		return CustomMsg[Header, Content]{
			Header: m.Header,
			Error:  err,
		}
	}
	return m
}
func (m CustomMsg[Header, Content]) DropContent() CustomMsg[Header, struct{}] {
	return CustomMsg[Header, struct{}]{
		Header: m.Header,
		Error:  m.Error,
	}
}
func MsgMap[Header, In, Out any](msg CustomMsg[Header, In], f func(In) (Out, error)) CustomMsg[Header, Out] {
	if msg.Error != nil {
		return CustomMsg[Header, Out]{
			Header: msg.Header,
			Error:  msg.Error,
		}
	}
	mapped, err := f(msg.Content)
	if err != nil {
		return CustomMsg[Header, Out]{
			Header: msg.Header,
			Error:  err,
		}
	}
	return CustomMsg[Header, Out]{
		Header:  msg.Header,
		Content: mapped,
	}
}
func MsgFilterMap[Header, In, Out any](msg CustomMsg[Header, In], f func(In) (Optional[Out], error)) (CustomMsg[Header, Out], bool) {
	if msg.Error != nil {
		return CustomMsg[Header, Out]{
			Header: msg.Header,
			Error:  msg.Error,
		}, true
	}
	mappedOpt, err := f(msg.Content)
	if err != nil {
		return CustomMsg[Header, Out]{
			Header: msg.Header,
			Error:  err,
		}, true
	}
	mapped, remain := mappedOpt.Unwrap()
	return CustomMsg[Header, Out]{
		Header:  msg.Header,
		Content: mapped,
	}, remain
}

// Standard Msg type, best decision for message processing
//
// So actual interface of the top-level producer can look like that:
//
//	type SomeProducerService[T any] interface {
//		// implements basic cluster.MsgProducer[T]:
//		Start(startupCtx context.Context, consumer Consumer[Msg[T]]) (context.CancelFunc, error)
//		// but also some other methods:
//		CommitProcessed(ctx context.Context, header Header) error
//		Recover(ctx context.Context, header Header) ([]Msg[T], error)
//	}
type Msg[T any] = CustomMsg[*Header, T]

// Standard header type
//
// Producer on start creates a new sequence, than start message emission from 0 state.
// This make actual indexing (like offset) incapsulated inside producer.
// Also we don't want to use cross session indexes - in some cases it will be extremely hard in implementation.
//
// StateAfter(Source.Start;Source.End) needed to handle sequence transformations without losing state data:
//   - Initial sequence: 1(0;1) 2(1;2) 3(2;3) 4(3;4) ...
//   - Batching [1(0;1) 2(1;2) 3(2;4)] => [3(0;4)] (3 updates batched into 1)
//   - Splitting [2(0;3)] => [0(0;3) 0(0;3) 0(0;3) 2(0;3)] (1 update splitted into 4 parts)
//   - Filtering [1(0;1) 2(1;2) 3(2;3)] => [1(0;1) 3(2;3)] (second update extracted)
//
// This helps to recover initial data on output and can be extremely useful for logging and configuring DLQ
// Of course there can be other approaches, but this one is the most intuitive
//
// Also it's helpful to note about expected guarantees:
//   - For each update i: Source.Start[i] <= StateAfter[i] <= Source.End[i]
//   - For each updates i < j: StateAfter[i] <= Source.Start[j]
//
// Because we use uint64 for indexation, we can assume that the numeric limit (overflow) will never be reached (this assume also used by Kafka)
// To be honest, I thinked about cycled uint32 for a long time (just for compactness) but easy comparsion seems very useful in some cases
type Header struct {
	NewState uint64 // new state

	Source struct {
		Start uint64
		End   uint64
	}

	SequenceId *SequenceId // identifier of the message sequence
}

type HeaderSource struct {
	Start uint64 // state of first affected message
	End   uint64 // state after last affected message
}

func (h *Header) String() string {
	return fmt.Sprintf("[%s]%v(%v;%v)", h.SequenceId, h.NewState, h.Source.Start, h.Source.End)
}

type SequenceId struct {
	SessionId  int64
	ProducerId int64
}

func (id *SequenceId) String() string {
	return fmt.Sprintf("%v;%v", id.ProducerId, id.SessionId)
}

type HeaderManager[ActualHeader any] interface {
	NewSession() (HeaderManagerSession[ActualHeader], error)
	GetSource(header *Header) ([]ActualHeader, error)
	// Standard way to commit success
	//
	// Will Ack all updates before header.Source.NewState
	AckHeader(
		header *Header,
		ackFunc func(headers []ActualHeader) error,
	) error
	// Optional method if your message broker supports DLQ (maybe self-configured) or Nacking (like RabbitMQ)
	//
	// Will Ack all updates before header.Source.Start and Nack all updates in [header.Source.Start; header.Source.End)
	NackHeaderSource(
		header *Header,
		ackFunc func(headers []ActualHeader) error,
		nackFunc func(headers []ActualHeader) error,
	) error
	CloseAll(nackFunc func(headers []ActualHeader) error) error
}

type HeaderManagerSession[ActualHeader any] interface {
	EmmitHeader(actual ActualHeader) (*Header, error)
	GetSequenceID() *SequenceId
	Close(closeFunc func(header []ActualHeader) error) error
	IsClosed() bool
}

func NewHeaderManager[ActualHeader any](allowParallelSessions bool) HeaderManager[ActualHeader] {
	return newHeaderManager[ActualHeader](allowParallelSessions)
}
