// Package ratelimit provides a token bucket rate limiter for HTTP servers.
//
// Rate limiting caps how many requests are accepted per unit of time. It is a
// temporal cap: the constraint is a rate (requests/second), not a size. This
// makes it distinct from two related resilience patterns:
//
//   - Bounded queue: a spatial cap on the backlog of pending work. It limits
//     how many in-flight items may accumulate, not how fast new ones arrive.
//     Once the queue is full, admission is refused (or blocks) regardless of
//     time.
//
//   - Bulkhead: partitioned isolation of resources (e.g. per-tenant worker
//     pools, per-dependency connection pools). It prevents one caller or
//     downstream from exhausting shared capacity by walling off a dedicated
//     slice. Bulkheads bound concurrency per partition, not rate.
//
// The Limiter implements the classic token bucket algorithm with lazy refill:
// tokens accrue continuously at a configured rate up to a fixed capacity, and
// each admitted request consumes one token. Bursts up to the capacity are
// allowed; sustained throughput converges to the refill rate. Refill is
// computed on demand from the elapsed wall-clock time, so no background
// goroutine is required.
package ratelimit

import (
	"net/http"
	"sync"
	"time"
)

// Limiter is a concurrency-safe token bucket rate limiter.
//
// A Limiter is created with New and shared across goroutines. Its zero value
// is not usable; always construct through New.
type Limiter struct {
	mu         sync.Mutex
	rate       float64   // tokens added per second
	capacity   float64   // maximum tokens the bucket may hold
	tokens     float64   // current token count (fractional, refilled lazily)
	lastRefill time.Time // last time tokens were recomputed
}

// New returns a Limiter that admits requests at the given steady-state rate
// (tokens per second) with the given burst capacity (maximum tokens in the
// bucket). The bucket starts full so the first burst up to capacity is
// admitted immediately.
//
// A non-positive rate or capacity yields a limiter that never admits requests.
func New(rate float64, capacity int) *Limiter {
	cap := float64(capacity)
	if rate < 0 {
		rate = 0
	}
	if cap < 0 {
		cap = 0
	}
	return &Limiter{
		rate:       rate,
		capacity:   cap,
		tokens:     cap,
		lastRefill: time.Now(),
	}
}

// Allow reports whether a single request may proceed at this instant. When it
// returns true, one token is consumed from the bucket. When it returns false,
// the bucket is empty and the caller should reject or defer the request.
//
// Allow is safe for concurrent use.
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.capacity {
			l.tokens = l.capacity
		}
	}
	l.lastRefill = now

	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// Middleware returns an http.Handler that enforces the rate limit before
// delegating to next. Requests that exceed the current bucket state receive
// an HTTP 429 Too Many Requests response with a plain text body; admitted
// requests are passed through unchanged.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
