// Package retry implements bounded retries with exponential backoff and
// full jitter around a faildep.Dependency call.
//
// Bounded: MaxAttempts caps total tries so a chronically failing dependency
// cannot pin a request forever.
//
// Exponential backoff: the delay window doubles each attempt
// (baseDelay * 2^attempt), giving the downstream time to recover before the
// next hit.
//
// Full jitter: the actual sleep is uniformly random in [0, cap). This
// spreads retries across the window and avoids the thundering-herd effect
// where synchronized clients all retry at the same instant after an outage.
// See "Exponential Backoff And Jitter" (AWS Architecture Blog).
//
// Idempotency caveat: retrying is only safe for idempotent operations
// (e.g. GET, or writes guarded by an idempotency key / conditional headers).
// Blindly retrying non-idempotent writes can duplicate side effects.
package retry

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

// Handler wraps a faildep.Dependency and retries failed calls up to
// MaxAttempts times, sleeping with exponential backoff + full jitter
// between attempts.
type Handler struct {
	dep         *faildep.Dependency
	maxAttempts int
	baseDelay   time.Duration
}

// New returns a Handler that will attempt dep.Call up to maxAttempts times,
// using baseDelay as the base of the exponential backoff window.
func New(dep *faildep.Dependency, maxAttempts int, baseDelay time.Duration) *Handler {
	return &Handler{
		dep:         dep,
		maxAttempts: maxAttempts,
		baseDelay:   baseDelay,
	}
}

// ServeHTTP calls the dependency up to h.maxAttempts times. Between attempts
// it sleeps for a duration drawn uniformly from [0, baseDelay*2^attempt),
// respecting request-context cancellation. On success it returns 200 OK; on
// all attempts exhausted (or ctx cancelled mid-backoff) it returns 502.
//
// Only retry idempotent operations (e.g. GET, or on timeout/5xx). Never
// blindly retry non-idempotent writes.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var lastErr error

	for attempt := 0; attempt < h.maxAttempts; attempt++ {
		if err := h.dep.Call(ctx); err == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		} else {
			lastErr = err
		}

		if attempt < h.maxAttempts-1 {
			// Full jitter backoff: cap = baseDelay * 2^attempt, sleep in [0, cap).
			// Spreads synchronized retriers to avoid thundering herd.
			cap := h.baseDelay << uint(attempt)
			sleep := time.Duration(rand.Int63n(int64(cap) + 1))
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				http.Error(w, "request cancelled: "+ctx.Err().Error(), http.StatusBadGateway)
				return
			}
		}
	}

	http.Error(w, "all retries exhausted: "+lastErr.Error(), http.StatusBadGateway)
}
