package disruptor

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type testEvent struct {
	producer int
	item     int
}

func TestRingBufferCapacityAndWraparound(t *testing.T) {
	ring := New[int](4)
	consumerSequence := NewSequence(-1)
	ring.AddGatingSequence(consumerSequence)

	for want := int64(0); want < 4; want++ {
		sequence, published := ring.Publish(int(want))
		if !published {
			t.Fatalf("Publish(%d) unexpectedly failed", want)
		}
		if sequence != want {
			t.Fatalf("sequence = %d, want %d", sequence, want)
		}
	}

	if _, published := ring.Publish(4); published {
		t.Fatal("publish succeeded while the ring was full")
	}

	for want := int64(0); want < 2; want++ {
		if got := ring.Get(want); got != int(want) {
			t.Fatalf("Get(%d) = %d, want %d", want, got, want)
		}
	}

	consumerSequence.Store(1)
	sequence, published := ring.Publish(4)
	if !published || sequence != 4 {
		t.Fatalf("Publish after consuming two events = (%d, %t), want (4, true)", sequence, published)
	}
	if got := ring.Get(4); got != 4 {
		t.Fatalf("Get(4) = %d, want 4", got)
	}
}

func TestConcurrentPublishAndConsume(t *testing.T) {
	const (
		producerCount    = 4
		itemsPerProducer = 500
		eventCount       = producerCount * itemsPerProducer
	)

	ring := New[testEvent](64)
	consumer := NewConsumer(ring, YieldWaitStrategy{})
	consumed := make(chan map[testEvent]struct{}, 1)

	go func() {
		seen := make(map[testEvent]struct{}, eventCount)
		for range eventCount {
			event := consumer.Next()
			seen[event] = struct{}{}
		}
		consumed <- seen
	}()

	var producers sync.WaitGroup
	producers.Add(producerCount)
	for producer := range producerCount {
		go func(producer int) {
			defer producers.Done()
			for item := range itemsPerProducer {
				for {
					if _, published := ring.Publish(testEvent{producer: producer, item: item}); published {
						break
					}
					runtime.Gosched()
				}
			}
		}(producer)
	}
	producers.Wait()

	select {
	case seen := <-consumed:
		if len(seen) != eventCount {
			t.Fatalf("consumed %d unique events, want %d", len(seen), eventCount)
		}
		for producer := range producerCount {
			for item := range itemsPerProducer {
				if _, ok := seen[testEvent{producer: producer, item: item}]; !ok {
					t.Errorf("missing event from producer %d, item %d", producer, item)
				}
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out consuming published events")
	}
}

func TestWaitStrategyHonorsDependencies(t *testing.T) {
	strategies := []WaitStrategy{
		YieldWaitStrategy{},
		BusySpinWaitStrategy{},
		NewSleepingWaitStrategy(time.Millisecond),
	}

	for _, strategy := range strategies {
		cursor := NewSequence(5)
		dependency := NewSequence(1)
		result := make(chan int64, 1)
		go func() {
			result <- strategy.WaitFor(2, cursor, dependency)
		}()

		dependency.Store(2)
		select {
		case available := <-result:
			if available != 2 {
				t.Errorf("%T returned %d, want 2", strategy, available)
			}
		case <-time.After(time.Second):
			t.Errorf("%T did not unblock when its dependency advanced", strategy)
		}
	}
}

func TestProcessBatchAdvancesConsumerSequence(t *testing.T) {
	ring := New[int](8)
	consumer := NewConsumer(ring, BusySpinWaitStrategy{})
	for value := 1; value <= 3; value++ {
		if _, published := ring.Publish(value); !published {
			t.Fatalf("Publish(%d) unexpectedly failed", value)
		}
	}

	var values []int
	consumer.ProcessBatch(func(value int) {
		values = append(values, value)
	})

	if len(values) != 3 || values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("ProcessBatch values = %v, want [1 2 3]", values)
	}
	if got := consumer.Sequence(); got != 2 {
		t.Fatalf("consumer sequence = %d, want 2", got)
	}
}

func TestDisruptorRunsEventsInSequenceAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []int
	disruptor, err := NewDisruptor(4, YieldWaitStrategy{}, func(_ context.Context, value int) error {
		processed = append(processed, value)
		if value == 9 {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NewDisruptor() error = %v", err)
	}

	runResult := make(chan error, 1)
	go func() {
		runResult <- disruptor.Run(ctx)
	}()

	for value := 0; value < 10; value++ {
		for {
			if _, published := disruptor.Publish(value); published {
				break
			}
			runtime.Gosched()
		}
	}

	select {
	case err := <-runResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after context cancellation")
	}

	if len(processed) != 10 {
		t.Fatalf("processed %d events, want 10", len(processed))
	}
	for want, got := range processed {
		if got != want {
			t.Fatalf("processed[%d] = %d, want %d", want, got, want)
		}
	}
}

func TestDisruptorCanRetryEventAfterHandlerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	disruptor, err := NewDisruptor(4, YieldWaitStrategy{}, func(_ context.Context, value int) error {
		calls++
		if calls == 1 {
			return errors.New("temporary handler failure")
		}
		if value != 42 {
			t.Errorf("handler value = %d, want 42", value)
		}
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("NewDisruptor() error = %v", err)
	}
	if _, published := disruptor.Publish(42); !published {
		t.Fatal("Publish(42) unexpectedly failed")
	}

	if err := disruptor.Run(ctx); err == nil || err.Error() != "temporary handler failure" {
		t.Fatalf("first Run() error = %v, want temporary handler failure", err)
	}
	if err := disruptor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Run() error = %v, want context.Canceled", err)
	}
	if calls != 2 {
		t.Fatalf("handler called %d times, want 2", calls)
	}
}

func TestNewDisruptorRejectsInvalidConfiguration(t *testing.T) {
	_, err := NewDisruptor[int](3, YieldWaitStrategy{}, func(context.Context, int) error { return nil })
	if !errors.Is(err, ErrInvalidDisruptorConfig) {
		t.Fatalf("NewDisruptor() error = %v, want ErrInvalidDisruptorConfig", err)
	}
}
