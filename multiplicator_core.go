package cluster

import "sync/atomic"

// immutable (so always async-safe) primitive for non-blocking sending
type multiplicatorCore[T any] interface {
	trySendToAll(message T) []error
	size() int
	nextAvailableKey(from int) int
	add(key int, receiver Sub[T]) multiplicatorCore[T]
	remove(key int, cause error) multiplicatorCore[T]
	close(cause error) multiplicatorCore[T]
}

const MultiplicatorReceiversLimit = 100000

type multiplicatorAtomicCore[T any] struct {
	atomic.Pointer[multiplicatorCore[T]]
}

func (c *multiplicatorAtomicCore[T]) get() multiplicatorCore[T] {
	res := c.Load()
	if res == nil {
		return nilCore[T]{}
	}
	return *res
}
func (c *multiplicatorAtomicCore[T]) set(new multiplicatorCore[T]) {
	c.Store(&new)
}

type nilCore[T any] struct{}

func (c nilCore[T]) trySendToAll(message T) []error {
	return nil
}
func (c nilCore[T]) size() int {
	return 0
}
func (c nilCore[T]) nextAvailableKey(from int) int {
	return from
}
func (c nilCore[T]) add(key int, receiver Sub[T]) multiplicatorCore[T] {
	return singleCore[T]{
		key:      key,
		receiver: receiver,
	}
}
func (c nilCore[T]) remove(_key int, _cause error) multiplicatorCore[T] {
	return c
}
func (c nilCore[T]) close(cause error) multiplicatorCore[T] {
	return c
}

type singleCore[T any] struct {
	key      int
	receiver Sub[T]
}

func (c singleCore[T]) trySendToAll(message T) []error {
	if err := c.receiver.TrySend(message); err != nil {
		return []error{err}
	}
	return nil
}
func (c singleCore[T]) size() int {
	return 1
}
func (c singleCore[T]) nextAvailableKey(from int) int {
	if c.key == from {
		return (from + 1) % MultiplicatorReceiversLimit
	}
	return from
}
func (c singleCore[T]) add(key int, receiver Sub[T]) multiplicatorCore[T] {
	if c.key == key {
		panic("logic error: key not available") // internal package error
	}
	new := singleCore[T]{
		key:      key,
		receiver: receiver,
	}
	if c.key < key {
		return multiCore[T]{c, new}
	}
	return multiCore[T]{new, c}
}
func (c singleCore[T]) remove(key int, cause error) multiplicatorCore[T] {
	if c.key != key {
		return c
	}
	c.receiver.Close(cause)
	return nilCore[T]{}
}
func (c singleCore[T]) close(cause error) multiplicatorCore[T] {
	c.receiver.Close(cause)
	return nilCore[T]{}
}

type multiCore[T any] []singleCore[T] // inv: sorted list with different keys

func (c multiCore[T]) trySendToAll(message T) []error {
	var errs []error
	for _, core := range c {
		if err := core.receiver.TrySend(message); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
func (c multiCore[T]) size() int {
	return len(c)
}
func (c multiCore[T]) nextAvailableKey(from int) int {
	if len(c) == MultiplicatorReceiversLimit {
		panic("logic error: receivers limit exceeded")
	}
	ind := c.findKeyIndOrNext(from) % len(c)
	for c[ind].key == from { // it's okay, method add has O(size) complexity anyway
		from++
		from %= MultiplicatorReceiversLimit
		ind++
		ind %= len(c)
	}
	return from
}
func (c multiCore[T]) add(key int, receiver Sub[T]) multiplicatorCore[T] {
	new := make(multiCore[T], 0, len(c)+1)
	added := false
	for _, core := range c {
		if core.key == key {
			panic("logic error: key not available") // internal package error
		}
		if !added && core.key > key {
			new = append(new, singleCore[T]{
				key:      key,
				receiver: receiver,
			})
			added = true
		}
		new = append(new, core)
	}
	if !added {
		new = append(new, singleCore[T]{
			key:      key,
			receiver: receiver,
		})
		added = true
	}
	return new
}
func (c multiCore[T]) remove(key int, cause error) multiplicatorCore[T] {
	ind := c.findKeyIndOrNext(key)
	if ind == len(c) || c[ind].key != key {
		return c
	}
	new := make(multiCore[T], 0, len(c)-1)
	for i, core := range c {
		if i == ind {
			core.receiver.Close(cause)
			continue
		}
		new = append(new, core)
	}
	if len(new) == 1 {
		return new[0]
	}
	return new
}
func (c multiCore[T]) close(cause error) multiplicatorCore[T] {
	for _, core := range c {
		core.receiver.Close(cause)
	}
	return nilCore[T]{}
}

// helper func
func (c multiCore[T]) findKeyIndOrNext(from int) int {
	l := -1
	r := len(c)
	for r-l > 1 {
		if m := (l + r) / 2; c[m].key < from {
			l = m
		} else {
			r = m
		}
	}
	return r
}
