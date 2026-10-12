package msc

import (
	"strconv"
	"sync"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	queue := New[int]()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queue.Enqueue(i)
	}
}

func BenchmarkDequeue(b *testing.B) {
	queue := New[int]()

	for i := 0; i < b.N; i++ {
		queue.Enqueue(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queue.Dequeue()
	}
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	queue := New[int]()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queue.Enqueue(i)
		queue.Dequeue()
	}
}

func BenchmarkConcurrentEnqueue(b *testing.B) {
	queue := New[int]()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Enqueue(1)
		}
	})
}

func BenchmarkConcurrentDequeue(b *testing.B) {
	queue := New[int]()

	for i := 0; i < b.N; i++ {
		queue.Enqueue(i)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Dequeue()
		}
	})
}

func BenchmarkConcurrentEnqueueDequeue(b *testing.B) {
	queue := New[int]()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Enqueue(1)
			queue.Dequeue()
		}
	})
}

func BenchmarkEnqueueString(b *testing.B) {
	queue := New[string]()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		queue.Enqueue(strconv.Itoa(i))
	}
}

func BenchmarkConcurrentEnqueueWithGoroutines(b *testing.B) {
	queue := New[int]()

	b.ResetTimer()

	var wg sync.WaitGroup
	workers := 8

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < b.N/workers; i++ {
				queue.Enqueue(i)
			}
		}()
	}

	wg.Wait()
}
