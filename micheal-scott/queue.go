package msc

import "sync/atomic"

type Node[T any] struct {
	value T
	next  atomic.Pointer[Node[T]]
}

type Queue[T any] struct {
	head atomic.Pointer[Node[T]]
	tail atomic.Pointer[Node[T]]
}

func New[T any]() *Queue[T] {
	dummy := &Node[T]{}

	q := &Queue[T]{}
	q.head.Store(dummy)
	q.tail.Store(dummy)
	return q
}

func (q *Queue[T]) Enqueue(value T) {
	node := &Node[T]{
		value: value,
	}

	for {
		tail := q.tail.Load()
		next := tail.next.Load()

		if tail != q.tail.Load() {
			continue
		}

		if next == nil {
			if tail.next.CompareAndSwap(nil, node) {
				q.tail.CompareAndSwap(tail, node)
				return
			}
		} else {
			q.tail.CompareAndSwap(tail, next)
		}
	}
}

func (q *Queue[T]) Dequeue() (T, bool) {
	for {
		head := q.head.Load()
		tail := q.tail.Load()
		next := head.next.Load()

		// Check that head is still the current head.
		if head != q.head.Load() {
			continue
		}

		if head == tail {
			if next == nil {
				var zero T
				return zero, false
			}

			// Tail is behind. Help move it forward.
			q.tail.CompareAndSwap(tail, next)

			continue
		}

		value := next.value

		if q.head.CompareAndSwap(head, next) {
			return value, true
		}
	}
}

func (q *Queue[T]) IsEmpty() bool {
	head := q.head.Load()
	return head.next.Load() == nil
}
