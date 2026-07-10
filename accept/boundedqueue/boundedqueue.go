// Package boundedqueue implements a bounded work queue backed by a buffered
// channel and a fixed pool of worker goroutines.
//
// The queue enforces a spatial backlog limit: at most `capacity` units of work
// may sit waiting for a worker at any given time. When the buffer is full,
// additional submissions are rejected immediately rather than blocking the
// caller. This provides backpressure and prevents unbounded memory growth
// under load.
//
// How this differs from neighbouring resilience patterns:
//
//   - Rate limiting (temporal): caps the number of operations per unit of
//     time (e.g. token bucket). It shapes throughput but does not, by itself,
//     bound the amount of work queued up behind the limiter.
//   - Bounded queue (spatial): caps the number of in-flight or queued
//     operations. It shapes memory and latency by rejecting once the buffer
//     is full, regardless of the rate at which requests arrive.
//   - Bulkhead (partitioned): isolates resources into separate compartments
//     so that saturation in one partition cannot starve the others. A
//     bulkhead is typically implemented on top of bounded queues, one per
//     partition.
//
// A single bounded queue applied to an HTTP handler acts as an admission
// controller: fast, non-blocking enqueue on success, 503 on overload.
package boundedqueue

import "net/http"

// Queue is a bounded work queue drained by a fixed set of worker goroutines.
//
// Work is submitted as parameterless functions on a buffered channel of size
// `capacity`. Exactly `workers` goroutines consume from the channel; each
// executes functions serially, so the maximum concurrency of submitted work
// equals `workers`.
type Queue struct {
	ch chan func()
}

// New constructs a Queue with the given buffer capacity and worker count,
// and starts the workers. Callers should size `capacity` to bound worst-case
// backlog and `workers` to bound worst-case concurrency of the work being
// executed.
//
// Both `capacity` and `workers` must be positive; the zero value of Queue is
// not usable.
func New(capacity, workers int) *Queue {
	q := &Queue{ch: make(chan func(), capacity)}
	for i := 0; i < workers; i++ {
		go func() {
			for fn := range q.ch {
				fn()
			}
		}()
	}
	return q
}

// Handler returns an http.Handler that attempts to enqueue `work` for
// asynchronous execution by the worker pool.
//
// Enqueue is non-blocking: if the internal buffer has room, the work is
// accepted and the handler responds 202 Accepted. If the buffer is full, the
// handler responds 503 Service Unavailable and drops the request. The work
// function itself is never executed on the request-handling goroutine, so
// response latency is independent of the work's cost.
func (q *Queue) Handler(work func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case q.ch <- work:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "queue full", http.StatusServiceUnavailable)
		}
	})
}
