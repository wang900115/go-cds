package disruptor

import (
	"sync"
	"sync/atomic"
)

type RingBuffer[T any] struct {
	events    []Event[T]
	published []atomic.Int64
	mask      int64

	gatingMu        sync.RWMutex
	nextSequence    atomic.Int64
	cursor          Sequence
	gatingSequences []*Sequence
}

// New creates a ring buffer whose size must be a power of two.
func New[T any](size int64) *RingBuffer[T] {
	if size <= 0 || size&(size-1) != 0 {
		panic("ring buffer size must be a power of two")
	}

	ring := &RingBuffer[T]{
		events:    make([]Event[T], size),
		published: make([]atomic.Int64, size),
		mask:      size - 1,
	}
	ring.cursor.Store(-1)
	for i := range ring.published {
		ring.published[i].Store(-1)
	}
	return ring
}

// AddGatingSequence registers a consumer sequence that limits slot reuse.
// Register consumers before publishing events.
func (r *RingBuffer[T]) AddGatingSequence(sequence *Sequence) {
	if sequence == nil {
		panic("gating sequence must not be nil")
	}

	r.gatingMu.Lock()
	defer r.gatingMu.Unlock()

	r.gatingSequences = append(r.gatingSequences, sequence)
}

func (r *RingBuffer[T]) MinimumGatingSequence() int64 {
	r.gatingMu.RLock()
	defer r.gatingMu.RUnlock()

	if len(r.gatingSequences) == 0 {
		return r.cursor.Load()
	}

	minimum := r.gatingSequences[0].Load()

	for _, sequence := range r.gatingSequences[1:] {
		value := sequence.Load()

		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

// Publish adds a value without blocking. It returns false when the ring is full.
func (r *RingBuffer[T]) Publish(value T) (int64, bool) {
	var sequence int64
	for {
		sequence = r.nextSequence.Load()
		if sequence-r.MinimumGatingSequence() > int64(len(r.events)) {
			return 0, false
		}
		if r.nextSequence.CompareAndSwap(sequence, sequence+1) {
			break
		}
	}

	index := sequence & r.mask
	r.events[index].Value = value
	r.published[index].Store(sequence)

	for {
		cursor := r.cursor.Load()
		next := cursor + 1
		if r.published[next&r.mask].Load() != next {
			break
		}
		if !r.cursor.CompareAndSwap(cursor, next) {
			continue
		}
	}

	return sequence, true
}

// Cursor returns the highest consecutively published sequence.
func (r *RingBuffer[T]) Cursor() int64 {
	return r.cursor.Load()
}

// Get returns the value stored at sequence. The sequence must still be in the ring.
func (r *RingBuffer[T]) Get(sequence int64) T {
	index := sequence & r.mask

	return r.events[index].Value
}
