### Go Cluster Pipes

# Introduction

Package contain some predefined pipes to implement function style adapters and other manipulations with cluster.EventPipe and cluster.MsgPipe

Message pipes commonly use error as an additional return argument, every error will be stored in out messages to future processing

Event pipes don't use errors, if you want to log some - you need to do it inside of transformation

All timeouts, retries and other configurations should be incapsulated in function

# Default (instant) pipes

Should be used ONLY if you want to apply non-blocking transformations

There are 4 possible kinds of transformation:
- Map (In -> Out)
- FilterMap (In -> (Out, ok))
- Filter (T -> (T, ok))
- ForEach (T -> T)

Probably there is no point to describe functionality - it's pretty obvious

# Async pipes

Async pipes transform "Task" entity instead of doing real work. Execution is a separate pipeline, which can be configured with AsyncExecutorConfig. So before and after async work you should use this pipes:
- AsyncPrepare (T -> Task[T]): wrap data in Task
- AsyncExecute (Task[T] -> (T, ok)): execute provided task in async mode without loosing the order

Task[T] has signature func(context.Context) (cluster.Optional[T], error)

Async pipes have common signatures:
- AsyncMap (Task[In] -> Task[Out]): append mapping to task
- AsyncFilterMap (Task[In] -> Task[Out]): append filtered mapping to task
- AsyncFilter (Task[T] -> Task[T]): append filtering to task
- AsyncForEach (Task[T] -> Task[T]): append call to task

# Sync pipes

Sync pipes process data in a single thread. To avoid slowing down by this, sync pipes should be configured with some batching and splitting afterwards (if needed):
- Batch (T -> []T): batch new data and forward it once in a SendTimeout
- Split ([]T -> T): split processed data back

If you don't want to implement batch calls, please, look at async pipes.

Sync pipes have common signatures:
- Map (In -> Out)
- FilterMap (In -> (Out, ok))
- Filter (T -> (T, ok))
- ForEach (T -> T)

# Other

- Broken pipes: return configured error on any usage try
- Recover message pipe: will run a subpipe, but forward original message on subpipe success or subpipe error on error
- Msg error handling pipes: can be used to configurate reaction on error messages (stop pipe/log error/drop message)

