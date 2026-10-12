# CDS (Concurrent Data Structure)
Concurrent Data Structures (CDS) are data structures designed to support concurrent access from multiple threads or goroutines while maintaining correctness and providing defined progress guarantees.
This repository explores the implementation and underlying mechanisms of several concurrent data structures and synchronization techniques in Go.

# Progress Guarantees

> [!WARNING]
> Progress Guarantee

`Blocking`

Threads may wait indefinitely because of locks or other blocking mechanisms

`Obstruction-Free`

A thread can make progress if it runs in isolation without contention

`Lock-Free`

The system as a whole always makes progress; at least one operation completes within a finite number of steps

`Wait-Free`

Every thread completes its operation within a bounded number of steps