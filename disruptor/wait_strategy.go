package disruptor

import (
	"context"
	"runtime"
	"time"
)

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependencies ...*Sequence) int64
}

// ContextWaitStrategy is implemented by wait strategies that can be cancelled.
type ContextWaitStrategy interface {
	WaitForContext(
		ctx context.Context,
		sequence int64,
		cursor *Sequence,
		dependencies ...*Sequence,
	) (int64, error)
}

func availableSequence(cursor *Sequence, dependencies []*Sequence) int64 {
	available := cursor.Load()

	for _, dependency := range dependencies {
		if value := dependency.Load(); value < available {
			available = value
		}
	}

	return available
}

func waitForContext(
	ctx context.Context,
	sequence int64,
	cursor *Sequence,
	dependencies []*Sequence,
	pause time.Duration,
) (int64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		available := availableSequence(cursor, dependencies)
		if available >= sequence {
			return available, nil
		}

		if pause == 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
				runtime.Gosched()
			}
			continue
		}

		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

type YieldWaitStrategy struct{}

func (YieldWaitStrategy) WaitFor(
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) int64 {
	for {
		available := availableSequence(cursor, dependencies)
		if available >= sequence {
			return available
		}

		runtime.Gosched()
	}
}

func (YieldWaitStrategy) WaitForContext(
	ctx context.Context,
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) (int64, error) {
	return waitForContext(ctx, sequence, cursor, dependencies, 0)
}

type BusySpinWaitStrategy struct{}

func (BusySpinWaitStrategy) WaitFor(
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) int64 {
	for {
		available := availableSequence(cursor, dependencies)
		if available >= sequence {
			return available
		}
	}
}

func (BusySpinWaitStrategy) WaitForContext(
	ctx context.Context,
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) (int64, error) {
	return waitForContext(ctx, sequence, cursor, dependencies, 0)
}

type SleepingWaitStrategy struct {
	sleep time.Duration
}

func NewSleepingWaitStrategy(
	sleep time.Duration,
) SleepingWaitStrategy {
	return SleepingWaitStrategy{
		sleep: sleep,
	}
}

func (s SleepingWaitStrategy) WaitFor(
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) int64 {
	for {
		available := availableSequence(cursor, dependencies)
		if available >= sequence {
			return available
		}

		time.Sleep(s.sleep)
	}
}

func (s SleepingWaitStrategy) WaitForContext(
	ctx context.Context,
	sequence int64,
	cursor *Sequence,
	dependencies ...*Sequence,
) (int64, error) {
	return waitForContext(ctx, sequence, cursor, dependencies, s.sleep)
}
