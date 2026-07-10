// Package bulkhead implements the bulkhead isolation pattern: concurrent
// callers are split into named partitions, each with its own bounded pool
// of in-flight slots. Overflow in one partition (e.g. a slow tenant, a
// noisy endpoint) cannot exhaust capacity used by other partitions.
//
// This is distinct from:
//   - Rate limiting, which caps requests per unit of TIME (temporal budget).
//   - A bounded queue, which is a single shared buffer for all callers.
//
// The bulkhead here caps CONCURRENCY per partition using a plain buffered
// channel as a counting semaphore: sending into the channel acquires a slot
// (non-blocking via select/default), receiving from it releases the slot.
// No third-party semaphore package is used.
package bulkhead

import (
	"net/http"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

// Bulkhead holds a set of named partitions. Each partition is an independent
// buffered channel semaphore; its capacity is the maximum number of
// concurrent in-flight calls allowed for that partition.
type Bulkhead struct {
	partitions map[string]chan struct{}
}

// New creates a Bulkhead with one buffered channel semaphore per entry in
// partitions. The map's value is the concurrency cap for that partition.
// Partitions with a cap <= 0 are created with capacity 0, which effectively
// disables the partition (all acquires will fail fast).
func New(partitions map[string]int) *Bulkhead {
	b := &Bulkhead{
		partitions: make(map[string]chan struct{}, len(partitions)),
	}
	for name, capacity := range partitions {
		if capacity < 0 {
			capacity = 0
		}
		b.partitions[name] = make(chan struct{}, capacity)
	}
	return b
}

// Handler returns an http.Handler that, for each request, tries to acquire
// a slot from the named partition's semaphore without blocking. If the
// partition is unknown or full, it responds with 503 Service Unavailable.
// Otherwise it invokes dep.Call with the request context, releases the slot
// on return, and reports 502 Bad Gateway on dependency error or 200 OK on
// success.
func (b *Bulkhead) Handler(partition string, dep *faildep.Dependency) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch, ok := b.partitions[partition]
		if !ok {
			http.Error(w, "unknown partition", http.StatusServiceUnavailable)
			return
		}

		select {
		case ch <- struct{}{}:
			defer func() { <-ch }()
		default:
			http.Error(w, "partition full", http.StatusServiceUnavailable)
			return
		}

		if err := dep.Call(r.Context()); err != nil {
			http.Error(w, "dependency failed", http.StatusBadGateway)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
