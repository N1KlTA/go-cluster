### Go Cluster

Framework for event-driven architecture, on very early stage

# Disclaimer

This project is in very early stage currently, some specifications can be changed in future, library totally need to grow and contain much more adapters (first of all, Kafka, Rabbit, Redis and NATS) and tests. I have some huge plans about it, i hope i will have enough time to implement it

Another problem: this library has some non-canonical decisions, flued with my influence in C++ and Rust freedom. These decisions can look wierd, but, if you will check, even some standard go packages have wierd implementations. All these decisions will look much more reasonable with "examples" folder, that i will add later.

And sorry for my very very bad english, yeah...

P.S. Even this README isn't complete yet, it's just a scratch

# Motivation

Event-driven systems are popular, very popular to be honest. But as long as there is some understanding about outer design of event-driven systems, inner microservice implementation commonly builds in a shitty imperative style. Why? Because actually there is no strict interface which will describe inner event processing. Many projects have some entry point from which a lot of different calls processed, with no clear pipeline and a lot of methods, that call other methods, that call some other methods and e.t.c. 

The main goal of this package is to provide simplified interfaces to configure consecutive pipelines instead of complicated dependecies. I want to make it easier to switch brokers/outer receivers without refactoring inner code itself. Event-driven system should be easier to design in my opinion.

# Basic info

There are two main entities, which process events/messages

1) Streams of events (pub/sub): guaranty at most once, pub don't know anything about subs (even their existence) and never waits for them. In other words, sender leads flow.
2) Sequences of messages (producer/consumer): guaranty at least once, producer (it can be an outer queue itself) waits for consumer to do it work and never skips updates. In other words, receiver leads flow.

# Streams of events

```go
type Stream[T any] interface {
    Subscribe(startupCtx context.Context, sub Sub[T]) (context.CancelFunc, error)
}

type Sub[T any] interface {
    TrySend(event T) error
    Close(cause error)
    IsClosed() bool
}

type EventPipe[In, Out any] func(startupCtx context.Context, sub Sub[Out]) (Sub[In], error)
func (p EventPipe[In, Out]) MapStream(stream Stream[In]) Stream[Out]
func (p EventPipe[In, Out]) MapSub(startupCtx context.Context, sub Sub[Out]) (Sub[In], error)

```

# Sequences of messages

```go
type CustomProducer[T any] interface {
    Start(startupCtx context.Context, consumer Consumer[T]) (context.CancelFunc, error)
}

type CustomConsumer[T any] interface {
    Send(ctx context.Context, message T) error
    Close(cause error)
    IsClosed() bool
}
```

But how to track the state? There Message type comes in:
```go
type CustomMessage[Header, Content any] struct {
    Header Header
    Body Content
    Err error
}
type Header struct {
	StateAfter uint64 // new state

	StartState uint64 // state of first affected message
	EndState   uint64 // state after last affected message

	SequenceId *SequenceId // identifier of message sequence
}
type Msg[T any] = CustomMessage[Header, T]
type MsgProducer[T any] = CustomProducer[Msg[T]]
type MsgConsumer[T any] = CustomConsumer[Msg[T]]
```

As long, as you can use any type in Queue and QueuePipe, it's highly recommended to use standard Message type, because only this type will be fully supported via this package

Final interface of the queue can be various (because actual Queue - like Kafka, Redis, RabbitMQ or NATS should be used only on the highest level) but standard interface will look like that:
```go
type StandardBrokerProducer[T any] interface {
    MsgProducer[T]
    Commit(ctx context.Context, header *Header) error // standard confirmation, confirm header and all previous
    GetSource(ctx context.Context, header *Header) ([]Message[T], error) // can be useful in case of custom DLQ or Tx commits
}
```

or like this:
```go
type CoolBrokerProducer[T any] interface {
    MsgProducer[T]
    Ack(ctx context.Context, header *Header) error
    Nack(ctx context.Context, header *Header, descr error) error
    NewDlqPipe(*DlqConfig) MsgPipe[struct{}, struct{}]
    GetSource(ctx context.Context, header *Header) ([]Message[T], error)
}
```

but, to be honest, in many cases it will be useful with this design:
```go
type SimpleProducer[State, T any] interface {
    NewProducer(getInitialState func(ctx context.Context) (State, error)) MsgProducer[T]
}
```

And this flexability is the key point of this package, new style of producer will only cause you to refactor root commit logic (your main.go script), not your pipes!