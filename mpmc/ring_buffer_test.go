package mpmc

import (
	"sync"
	"testing"
)

func TestRingBuffer(t *testing.T) {
	t.Run("enqueue and dequeue", func(t *testing.T) {
		queue := New[int](4)

		if !queue.Enqueue(1) {
			t.Fatal("enqueue 1 failed")
		}

		if !queue.Enqueue(2) {
			t.Fatal("enqueue 2 failed")
		}

		if !queue.Enqueue(3) {
			t.Fatal("enqueue 3 failed")
		}

		value, ok := queue.Dequeue()
		if !ok || value != 1 {
			t.Fatalf("expected 1, got %v", value)
		}

		value, ok = queue.Dequeue()
		if !ok || value != 2 {
			t.Fatalf("expected 2, got %v", value)
		}

		value, ok = queue.Dequeue()
		if !ok || value != 3 {
			t.Fatalf("expected 3, got %v", value)
		}
	})

	t.Run("empty", func(t *testing.T) {
		queue := New[int](4)

		value, ok := queue.Dequeue()

		if ok {
			t.Fatal("expected dequeue to fail")
		}

		if value != 0 {
			t.Fatalf("expected zero value, got %v", value)
		}
	})

	t.Run("full", func(t *testing.T) {
		queue := New[int](4)

		for i := 0; i < 4; i++ {
			if !queue.Enqueue(i) {
				t.Fatalf("enqueue %d failed", i)
			}
		}

		if queue.Enqueue(4) {
			t.Fatal("expected enqueue to fail when queue is full")
		}
	})

	t.Run("wrap around", func(t *testing.T) {
		queue := New[int](4)

		for i := 0; i < 4; i++ {
			if !queue.Enqueue(i) {
				t.Fatalf("enqueue %d failed", i)
			}
		}

		for i := 0; i < 4; i++ {
			value, ok := queue.Dequeue()

			if !ok {
				t.Fatalf("dequeue %d failed", i)
			}

			if value != i {
				t.Fatalf("expected %d, got %d", i, value)
			}
		}

		for i := 4; i < 8; i++ {
			if !queue.Enqueue(i) {
				t.Fatalf("enqueue %d failed after wrap around", i)
			}
		}

		for i := 4; i < 8; i++ {
			value, ok := queue.Dequeue()

			if !ok {
				t.Fatalf("dequeue %d failed after wrap around", i)
			}

			if value != i {
				t.Fatalf("expected %d, got %d", i, value)
			}
		}
	})

	t.Run("enqueue nil", func(t *testing.T) {
		queue := New[*int](4)

		if !queue.Enqueue(nil) {
			t.Fatal("enqueue nil failed")
		}

		value, ok := queue.Dequeue()

		if !ok {
			t.Fatal("dequeue failed")
		}

		if value != nil {
			t.Fatalf("expected nil, got %v", value)
		}
	})
}

func TestNew(t *testing.T) {
	t.Run("invalid capacity", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()

		New[int](3)
	})

	t.Run("zero capacity", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()

		New[int](0)
	})

	t.Run("power of two", func(t *testing.T) {
		for _, capacity := range []uint64{1, 2, 4, 8, 16, 32} {
			queue := New[int](capacity)

			if queue == nil {
				t.Fatalf("queue is nil for capacity %d", capacity)
			}
		}
	})
}

func TestConcurrent(t *testing.T) {
	t.Run("multiple producers", func(t *testing.T) {
		queue := New[int](8192)

		const (
			producers = 8
			items     = 1000
		)

		var wg sync.WaitGroup
		wg.Add(producers)

		for producer := 0; producer < producers; producer++ {
			go func(id int) {
				defer wg.Done()

				for i := 0; i < items; i++ {
					for !queue.Enqueue(id*items + i) {
					}
				}
			}(producer)
		}

		wg.Wait()

		seen := make(map[int]bool)

		for i := 0; i < producers*items; i++ {
			value, ok := queue.Dequeue()

			if !ok {
				t.Fatalf("dequeue failed at index %d", i)
			}

			if seen[value] {
				t.Fatalf("duplicate value: %d", value)
			}

			seen[value] = true
		}
	})

	t.Run("multiple consumers", func(t *testing.T) {
		queue := New[int](8192)

		const (
			consumers = 8
			items     = 1000
		)

		for i := 0; i < consumers*items; i++ {
			for !queue.Enqueue(i) {
				t.Fatalf("enqueue %d failed", i)
			}
		}

		seen := make([]bool, 8000)

		var (
			wg sync.WaitGroup
			mu sync.Mutex
		)

		wg.Add(consumers)

		for i := 0; i < consumers; i++ {
			go func() {
				defer wg.Done()

				for {
					value, ok := queue.Dequeue()

					if !ok {
						return
					}

					mu.Lock()

					if seen[value] {
						t.Errorf("duplicate value: %d", value)
					}

					seen[value] = true

					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		for i, exists := range seen {
			if !exists {
				t.Fatalf("value %d was not dequeued", i)
			}
		}
	})

	t.Run("multiple producers and consumers", func(t *testing.T) {
		queue := New[int](1024)

		const (
			producers = 4
			consumers = 4
			items     = 5000
		)

		var producerWG sync.WaitGroup
		var consumerWG sync.WaitGroup

		results := make(chan int, producers*items)

		producerWG.Add(producers)

		for producer := 0; producer < producers; producer++ {
			go func(id int) {
				defer producerWG.Done()

				for i := 0; i < items; i++ {
					value := id*items + i

					for !queue.Enqueue(value) {
					}
				}
			}(producer)
		}

		consumerWG.Add(consumers)

		for i := 0; i < consumers; i++ {
			go func() {
				defer consumerWG.Done()

				for {
					value, ok := queue.Dequeue()

					if ok {
						results <- value
						continue
					}

					if queue.dequeuePos.Load() >= uint64(producers*items) {
						return
					}
				}
			}()
		}

		producerWG.Wait()
		consumerWG.Wait()

		close(results)

		seen := make(map[int]bool)

		for value := range results {
			if seen[value] {
				t.Fatalf("duplicate value: %d", value)
			}

			seen[value] = true
		}

		if len(seen) != producers*items {
			t.Fatalf(
				"expected %d values, got %d",
				producers*items,
				len(seen),
			)
		}
	})
}
