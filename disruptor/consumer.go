package disruptor

import "context"

type Consumer[T any] struct {
	ring     *RingBuffer[T]
	barrier  *SequenceBarrier
	sequence Sequence // consumer processed
}

// NewConsumer creates a consumer starting before the first event.
func NewConsumer[T any](ring *RingBuffer[T], waitStrategy WaitStrategy, dependencies ...*Sequence) *Consumer[T] {
	if ring == nil {
		panic("ring buffer must not be nil")
	}

	c := &Consumer[T]{
		ring:    ring,
		barrier: NewSequenceBarrier(&ring.cursor, waitStrategy, dependencies...),
	}
	c.sequence.Store(-1)

	ring.AddGatingSequence(&c.sequence)

	return c
}

// Sequence returns the last consumed sequence.
func (c *Consumer[T]) Sequence() int64 {
	return c.sequence.Load()
}

// Next waits for and returns the next event.
func (c *Consumer[T]) Next() T {
	next := c.sequence.Load() + 1

	c.barrier.WaitFor(next)

	value := c.ring.Get(next)

	// Mark the event as consumed.
	c.sequence.Store(next)

	return value
}

// ProcessNext waits for and processes one event, advancing the consumer
// sequence only after the handler succeeds.
func (c *Consumer[T]) ProcessNext(
	ctx context.Context,
	handler func(context.Context, T) error,
) error {
	next := c.sequence.Load() + 1
	if _, err := c.barrier.WaitForContext(ctx, next); err != nil {
		return err
	}

	if err := handler(ctx, c.ring.Get(next)); err != nil {
		return err
	}
	c.sequence.Store(next)
	return nil
}

func (c *Consumer[T]) ProcessBatch(
	handler func(T),
) {
	next := c.sequence.Load() + 1

	available := c.barrier.WaitFor(next)

	for sequence := next; sequence <= available; sequence++ {
		value := c.ring.Get(sequence)

		handler(value)
	}

	// Publish consumer progress once.
	c.sequence.Store(available)
}
