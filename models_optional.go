package cluster

// I didn't want to create an Optional, it isn't go style at all
// But, unfortunately, if you want to create FilterMap syntax it's inevitable
// Yes, you can use func() (T, bool, error) instead but it look way too bad
// So i implemented Optional

type Optional[T any] struct {
	val T
	ok  bool
}

func AsOptional[T any](val T, ok bool) Optional[T] {
	return Optional[T]{
		val: val,
		ok:  ok,
	}
}
func Some[T any](val T) Optional[T] {
	return Optional[T]{
		val: val,
		ok:  true,
	}
}
func None[T any]() Optional[T] {
	return Optional[T]{}
}

func (o Optional[T]) Filter(f func(T) bool) Optional[T] {
	if !o.ok || !f(o.val) {
		return None[T]()
	}
	return o
}
func (o Optional[T]) Map(f func(T) T) Optional[T] {
	if !o.ok {
		return None[T]()
	}
	return Some(f(o.val))
}
func (o Optional[T]) FilterMap(f func(T) (T, bool)) Optional[T] {
	if !o.ok {
		return None[T]()
	}
	return AsOptional(f(o.val))
}
func (o Optional[T]) Then(f func(T)) Optional[T] {
	return o
}
func (o Optional[T]) Unwrap() (T, bool) {
	return o.val, o.ok
}
func (o Optional[T]) IsNone() bool {
	return !o.ok
}

func OptionalMap[In, Out any](o Optional[In], f func(In) Out) Optional[Out] {
	if !o.ok {
		return None[Out]()
	}
	return Some(f(o.val))
}
func OptionalFilterMap[In, Out any](o Optional[In], f func(In) (Out, bool)) Optional[Out] {
	if !o.ok {
		return None[Out]()
	}
	return AsOptional(f(o.val))
}
