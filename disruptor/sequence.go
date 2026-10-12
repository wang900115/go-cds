package disruptor

import "sync/atomic"

type Sequence struct {
	_ [7]int64

	value atomic.Int64

	_ [7]int64
}

func NewSequence(value int64) *Sequence {
	s := &Sequence{}
	s.value.Store(value)
	return s
}

func (s *Sequence) Load() int64 {
	return s.value.Load()
}

func (s *Sequence) Store(value int64) {
	s.value.Store(value)
}

func (s *Sequence) CompareAndSwap(old, new int64) bool {
	return s.value.CompareAndSwap(old, new)
}
