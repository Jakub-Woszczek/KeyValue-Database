package datastructures

type Stack[T any] []T

func (s *Stack[T]) Push(v T) {
	*s = append(*s, v)
}

func (s *Stack[T]) Pop() (T, bool) {
	var zero T
	if len(*s) == 0 {
		return zero, false
	}

	n := len(*s) - 1
	v := (*s)[n]
	(*s)[n] = zero
	*s = (*s)[:n]
	return v, true
}

func (s *Stack[T]) Peek() (T, bool) {
	if len(*s) == 0 {
		var zero T
		return zero, false
	}
	return (*s)[len(*s)-1], true
}

func (s *Stack[T]) Len() int {
	return len(*s)
}
