package disruptor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrInvalidDisruptorConfig  = errors.New("invalid disruptor configuration")
	ErrDisruptorAlreadyRunning = errors.New("disruptor is already running")
)

// EventHandler processes events in ring-buffer sequence order.
type EventHandler[T any] func(context.Context, T) error

// Disruptor provides a multi-producer, single-consumer event pipeline.
type Disruptor[T any] struct {
	ring     *RingBuffer[T]
	consumer *Consumer[T]
	handler  EventHandler[T]
	running  atomic.Bool
}

// NewDisruptor creates a disruptor with the requested ring size and event handler.
func NewDisruptor[T any](
	size int64,
	waitStrategy WaitStrategy,
	handler EventHandler[T],
) (*Disruptor[T], error) {
	if size <= 0 || size&(size-1) != 0 {
		return nil, fmt.Errorf("%w: ring size must be a positive power of two", ErrInvalidDisruptorConfig)
	}
	if waitStrategy == nil {
		return nil, fmt.Errorf("%w: wait strategy must not be nil", ErrInvalidDisruptorConfig)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: event handler must not be nil", ErrInvalidDisruptorConfig)
	}

	ring := New[T](size)
	return &Disruptor[T]{
		ring:     ring,
		consumer: NewConsumer(ring, waitStrategy),
		handler:  handler,
	}, nil
}

// Publish adds an event without blocking. It returns false when the ring is full.
func (d *Disruptor[T]) Publish(event T) (int64, bool) {
	return d.ring.Publish(event)
}

// Run processes events sequentially until ctx is cancelled or the handler fails.
// A failed event remains unacknowledged and can be retried by calling Run again.
func (d *Disruptor[T]) Run(ctx context.Context) error {
	if !d.running.CompareAndSwap(false, true) {
		return ErrDisruptorAlreadyRunning
	}
	defer d.running.Store(false)

	for {
		if err := d.consumer.ProcessNext(ctx, d.handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}
