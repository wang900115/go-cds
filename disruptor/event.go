package disruptor

type Event[T any] struct {
	Value T
}
