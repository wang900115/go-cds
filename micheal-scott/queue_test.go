package msc

import (
	"sync"
	"testing"
)

func TestQueue(t *testing.T) {
	t.Run("enqueue", func(t *testing.T) {
		q := New[int]()

		q.Enqueue(1)
		q.Enqueue(2)
		q.Enqueue(3)

		if q.IsEmpty() {
			t.Fatal("queue should not be empty")
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		q := New[int]()

		q.Enqueue(1)
		q.Enqueue(2)
		q.Enqueue(3)

		got, ok := q.Dequeue()
		if !ok {
			t.Fatal("expected value")
		}

		if got != 1 {
			t.Fatalf("got %d, want 1", got)
		}

		got, ok = q.Dequeue()
		if !ok {
			t.Fatal("expected value")
		}

		if got != 2 {
			t.Fatalf("got %d, want 2", got)
		}

		got, ok = q.Dequeue()
		if !ok {
			t.Fatal("expected value")
		}

		if got != 3 {
			t.Fatalf("got %d, want 3", got)
		}
	})

	t.Run("enqueue nil", func(t *testing.T) {
		q := New[*int]()

		q.Enqueue(nil)

		got, ok := q.Dequeue()
		if !ok {
			t.Fatal("expected value")
		}

		if got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})

	t.Run("dequeue empty", func(t *testing.T) {
		q := New[int]()

		got, ok := q.Dequeue()

		if ok {
			t.Fatal("expected queue to be empty")
		}

		if got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})
}

func TestConcurrent(t *testing.T) {
	t.Run("concurrent enqueue", func(t *testing.T) {
		q := New[int]()

		const (
			goroutines = 8
			iterations = 1_000
		)

		var wg sync.WaitGroup

		for i := 0; i < goroutines; i++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for j := 0; j < iterations; j++ {
					q.Enqueue(id*iterations + j)
				}
			}(i)
		}

		wg.Wait()

		count := 0

		for {
			_, ok := q.Dequeue()
			if !ok {
				break
			}

			count++
		}

		want := goroutines * iterations

		if count != want {
			t.Fatalf("dequeued %d values, want %d", count, want)
		}
	})

	t.Run("concurrent dequeue", func(t *testing.T) {
		q := New[int]()

		const items = 8_000

		for i := 0; i < items; i++ {
			q.Enqueue(i)
		}

		const goroutines = 8

		var wg sync.WaitGroup
		var mu sync.Mutex

		values := make(map[int]struct{})

		for i := 0; i < goroutines; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for {
					value, ok := q.Dequeue()
					if !ok {
						return
					}

					mu.Lock()
					values[value] = struct{}{}
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		if len(values) != items {
			t.Fatalf(
				"dequeued %d unique values, want %d",
				len(values),
				items,
			)
		}

		if !q.IsEmpty() {
			t.Fatal("queue should be empty")
		}
	})
}
