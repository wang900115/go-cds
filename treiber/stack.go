package treiber

import "sync/atomic"

type Node[T any] struct {
	value T
	next  *Node[T]
}

type Stack[T any] struct {
	top atomic.Pointer[Node[T]]
}

func New[T any]() *Stack[T] {
	return &Stack[T]{}
}

func (s *Stack[T]) Push(value T) {
	node := &Node[T]{
		value: value,
	}

	for {
		oldTop := s.top.Load()

		node.next = oldTop

		// successfully swap if s.top equal oldTop
		if s.top.CompareAndSwap(oldTop, node) {
			return
		}
	}
}

func (s *Stack[T]) Pop() (T, bool) {
	for {
		oldTop := s.top.Load()

		if oldTop == nil {
			var zero T
			return zero, false
		}

		newTop := oldTop.next

		// successfully swap if s.top equal oldTop
		if s.top.CompareAndSwap(oldTop, newTop) {
			return oldTop.value, true
		}
	}
}

func (s *Stack[T]) IsEmpty() bool {
	return s.top.Load() == nil
}
