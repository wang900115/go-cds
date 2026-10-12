package treiber

import (
	"sync"
	"testing"
)

func BenchmarkStackPush(b *testing.B) {
	stack := New[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stack.Push(i)
	}
}

func BenchmarkStackPop(b *testing.B) {
	stack := New[int]()
	for i := 0; i < b.N; i++ {
		stack.Push(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stack.Pop()
	}
}

func BenchmarkStackParallel(b *testing.B) {
	stack := New[int]()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stack.Push(1)
			stack.Pop()
		}
	})
}

func BenchmarkStackMutex(b *testing.B) {
	var (
		mu    sync.Mutex
		stack []int
	)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			stack = append(stack, 1)
			stack = stack[:len(stack)-1]
			mu.Unlock()
		}
	})
}
