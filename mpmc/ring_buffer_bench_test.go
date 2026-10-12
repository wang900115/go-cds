package mpmc

import (
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	queue := New[int](1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for !queue.Enqueue(i) {
			queue.Dequeue()
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	queue := New[int](1024)

	for i := 0; i < b.N; i++ {
		queue.Enqueue(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for {
			if _, ok := queue.Dequeue(); ok {
				break
			}
		}

		queue.Enqueue(i)
	}
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	queue := New[int](1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for !queue.Enqueue(i) {
		}

		for {
			if _, ok := queue.Dequeue(); ok {
				break
			}
		}
	}
}

func BenchmarkMPMC(b *testing.B) {
	queue := New[int](1024)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for !queue.Enqueue(1) {
			}

			for {
				if _, ok := queue.Dequeue(); ok {
					break
				}
			}
		}
	})
}
