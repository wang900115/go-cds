package treiber

import (
	"sync"
	"testing"
)

func TestStack(t *testing.T) {
	t.Run("push", func(t *testing.T) {
		stack := New[int]()

		stack.Push(1)
		stack.Push(2)
		stack.Push(3)

		if stack.IsEmpty() {
			t.Fatal("stack should not be empty")
		}
	})

	t.Run("pop", func(t *testing.T) {
		stack := New[int]()

		stack.Push(1)
		stack.Push(2)
		stack.Push(3)

		got, ok := stack.Pop()
		if !ok {
			t.Fatal("expected value")
		}

		if got != 3 {
			t.Fatalf("got %d, want 3", got)
		}

		got, ok = stack.Pop()
		if !ok {
			t.Fatal("expected value")
		}

		if got != 2 {
			t.Fatalf("got %d, want 2", got)
		}
	})

	t.Run("push nil", func(t *testing.T) {
		stack := New[*int]()

		stack.Push(nil)

		got, ok := stack.Pop()
		if !ok {
			t.Fatal("expected value")
		}

		if got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})

	t.Run("pop empty", func(t *testing.T) {
		stack := New[int]()

		got, ok := stack.Pop()

		if ok {
			t.Fatal("expected pop to fail")
		}

		if got != 0 {
			t.Fatalf("got %d, want zero value", got)
		}
	})
}

func TestConcurrent(t *testing.T) {
	t.Run("concurrent push", func(t *testing.T) {
		stack := New[int]()

		const (
			goroutines = 100
			iterations = 1_000
		)

		var wg sync.WaitGroup

		for i := 0; i < goroutines; i++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				for j := 0; j < iterations; j++ {
					stack.Push(id*iterations + j)
				}
			}(i)
		}

		wg.Wait()

		count := 0

		for {
			_, ok := stack.Pop()
			if !ok {
				break
			}

			count++
		}

		want := goroutines * iterations

		if count != want {
			t.Fatalf("popped %d values, want %d", count, want)
		}
	})

	t.Run("concurrent pop", func(t *testing.T) {
		stack := New[int]()

		const (
			goroutines = 100
			items      = 8_000
		)

		for i := 0; i < items; i++ {
			stack.Push(i)
		}

		var wg sync.WaitGroup
		var mu sync.Mutex

		popped := make(map[int]struct{})

		for i := 0; i < goroutines; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for {
					value, ok := stack.Pop()
					if !ok {
						return
					}

					mu.Lock()
					popped[value] = struct{}{}
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		if len(popped) != items {
			t.Fatalf(
				"popped %d unique values, want %d",
				len(popped),
				items,
			)
		}

		if !stack.IsEmpty() {
			t.Fatal("stack should be empty")
		}
	})
}
