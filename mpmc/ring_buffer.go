package mpmc

import "sync/atomic"

type cell[T any] struct {
	sequence atomic.Uint64
	value    T
}

type RingBuffer[T any] struct {
	buffer []cell[T]
	mask   uint64

	enqueuePos atomic.Uint64
	dequeuePos atomic.Uint64
}

func New[T any](capacity uint64) *RingBuffer[T] {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		panic("capacity must be a power of two")
	}

	buffer := make([]cell[T], capacity)

	for i := uint64(0); i < uint64(capacity); i++ {
		buffer[i].sequence.Store(i)
	}

	return &RingBuffer[T]{
		buffer: buffer,
		mask:   capacity - 1,
	}
}

func (q *RingBuffer[T]) Enqueue(value T) bool {
	for {
		pos := q.enqueuePos.Load()

		cell := &q.buffer[pos&q.mask]
		sequence := cell.sequence.Load()

		difference := int64(sequence - pos)

		if difference == 0 {
			if q.enqueuePos.CompareAndSwap(pos, pos+1) {
				cell.value = value
				cell.sequence.Store(pos + 1)
				return true
			}
			continue
		}
		if difference < 0 {
			return false
		}
	}
}

func (q *RingBuffer[T]) Dequeue() (T, bool) {
	for {
		pos := q.dequeuePos.Load()

		cell := &q.buffer[pos&q.mask]
		sequence := cell.sequence.Load()

		difference := int64(sequence - (pos + 1))

		if difference == 0 {
			if q.dequeuePos.CompareAndSwap(pos, pos+1) {
				value := cell.value
				cell.sequence.Store(pos + q.mask + 1)
				return value, true
			}

			continue
		}

		if difference < 0 {
			var zero T
			return zero, false
		}
	}
}
