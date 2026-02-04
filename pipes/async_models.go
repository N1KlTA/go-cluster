package clusterpipes

import (
	"context"
	"errors"
	"time"

	"github.com/N1KlTA/go-cluster"
)

// Async execution config sets limits for spawned tasks
//
// You can use it .yaml config of your service
// Check README.md or async pipes to read more about async mechanism
type AsyncExecutorConfig struct {
	// Execution buffer sets maximal number of promises, that can be stored
	//
	// In practice that means maximal number of workers (active goroutines)
	// Default value equal to 40
	ExecutionBuffer int `yaml:"executionBuffer"`
	// Task timeout is a timeout that will be applied to each task
	//
	// If you want no timeout for tasks - you need to explicitly set the value to -1
	// Default value equal to 1 second
	TaskTimeout time.Duration `yaml:"taskTimeout"`
}

const (
	executionBufferDefault = 40
	taskTimeoutDefault     = 1 * time.Second
	taskTimeoutUnlimited   = -1
)

func (c *AsyncExecutorConfig) getExecutionBuffer() int {
	if c.ExecutionBuffer == 0 {
		return executionBufferDefault
	}
	return c.ExecutionBuffer
}
func (c *AsyncExecutorConfig) getTimeout() (time.Duration, bool) {
	if c.TaskTimeout == taskTimeoutUnlimited {
		return 0, false
	}
	if c.TaskTimeout == 0 {
		return taskTimeoutDefault, true
	}
	return c.TaskTimeout, true
}

func NewTask[T any](data T) Task[T] {
	return func(_ context.Context) (cluster.Optional[T], error) {
		return cluster.Some(data), nil
	}
}

var ErrNilTask = errors.New("nil task provided")

type Task[T any] func(context.Context) (cluster.Optional[T], error)

func (t Task[T]) Do(ctx context.Context) (cluster.Optional[T], error) {
	if t == nil {
		return cluster.None[T](), ErrNilTask
	}
	return t(ctx)
}
func (t Task[T]) Detach(ctx context.Context) Promise[cluster.Optional[T]] {
	return DetachFunc(ctx, t.Do)
}
func (t Task[T]) WithTimeout(timeout time.Duration) Task[T] {
	if t == nil {
		return nil
	}
	return func(ctx context.Context) (cluster.Optional[T], error) {
		newCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return t(newCtx)
	}
}
func (t Task[T]) Map(f func(context.Context, T) (T, error)) Task[T] {
	return MapTask(t, f)
}
func (t Task[T]) FilterMap(f func(context.Context, T) (cluster.Optional[T], error)) Task[T] {
	return FilterMapTask(t, f)
}
func (t Task[T]) Then(f func(context.Context, T) error) Task[T] {
	if t == nil {
		return nil
	}
	return func(ctx context.Context) (cluster.Optional[T], error) {
		opt, err := t(ctx)
		val, ok := opt.Unwrap()
		if err != nil || !ok {
			return cluster.None[T](), err
		}
		if err = f(ctx, val); err != nil {
			return cluster.None[T](), err
		}
		return cluster.Some(val), nil
	}
}
func (t Task[T]) Filter(f func(context.Context, T) (bool, error)) Task[T] {
	return FilterMapTask(t, func(ctx context.Context, data T) (cluster.Optional[T], error) {
		remain, err := f(ctx, data)
		if err != nil || !remain {
			return cluster.None[T](), err
		}
		return cluster.Some(data), nil
	})
}
func MapTask[In, Out any](task Task[In], f func(context.Context, In) (Out, error)) Task[Out] {
	return FilterMapTask(task, func(ctx context.Context, data In) (cluster.Optional[Out], error) {
		res, err := f(ctx, data)
		if err != nil {
			return cluster.None[Out](), err
		}
		return cluster.Some(res), nil
	})
}
func FilterMapTask[In, Out any](task Task[In], f func(context.Context, In) (cluster.Optional[Out], error)) Task[Out] {
	if task == nil {
		return nil
	}
	return func(ctx context.Context) (cluster.Optional[Out], error) {
		midOpt, err := task(ctx)
		mid, ok := midOpt.Unwrap()
		if err != nil || !ok {
			return cluster.None[Out](), err
		}
		return f(ctx, mid)
	}
}

// Basic promise interface
type Promise[T any] interface {
	// return done channel that will be closed when work will be done
	Await() <-chan struct{}
	// will block until await closed and return promised result
	Get() (T, error)
}

func DetachFunc[T any](ctx context.Context, f func(context.Context) (T, error)) Promise[T] {
	res := &simplePromise[T]{
		done: make(chan struct{}),
	}
	go func(ctx context.Context) {
		defer close(res.done)
		res.res, res.err = f(ctx)
	}(ctx)
	return res
}

// very very naive promise implementation
type simplePromise[T any] struct {
	res  T     // write guarded outside, read guarded by done
	err  error // write guarded outside, read guarded by done
	done chan struct{}
}

func (p *simplePromise[T]) Await() <-chan struct{} {
	return p.done
}
func (p *simplePromise[T]) Get() (T, error) {
	<-p.done
	return p.res, p.err
}
