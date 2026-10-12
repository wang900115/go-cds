package disruptor

import (
	"context"
	"runtime"
)

type SequenceBarrier struct {
	dependencies []*Sequence
	cursor       *Sequence
	waitStrategy WaitStrategy
}

func NewSequenceBarrier(cursor *Sequence, waitStrategy WaitStrategy, dependencies ...*Sequence) *SequenceBarrier {
	if cursor == nil {
		panic("barrier cursor must not be nil")
	}
	if waitStrategy == nil {
		panic("wait strategy must not be nil")
	}
	for _, dependency := range dependencies {
		if dependency == nil {
			panic("barrier dependency must not be nil")
		}
	}

	return &SequenceBarrier{
		cursor:       cursor,
		dependencies: dependencies,
		waitStrategy: waitStrategy,
	}
}

func (b *SequenceBarrier) WaitFor(sequence int64) int64 {
	return b.waitStrategy.WaitFor(sequence, b.cursor, b.dependencies...)
}

func (b *SequenceBarrier) WaitForContext(ctx context.Context, sequence int64) (int64, error) {
	if strategy, ok := b.waitStrategy.(ContextWaitStrategy); ok {
		return strategy.WaitForContext(ctx, sequence, b.cursor, b.dependencies...)
	}

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		available := availableSequence(b.cursor, b.dependencies)
		if available >= sequence {
			return available, nil
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			runtime.Gosched()
		}
	}
}
