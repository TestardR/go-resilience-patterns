// Package circuitbreaker implements the full three-state circuit breaker
// around a faildep.Dependency call: Closed, Open, and Half-Open.
//
// Closed: the normal state. Every request is forwarded to the dependency.
// Successful calls keep the breaker closed and reset the failure counter;
// each failure increments the counter. Once consecutive failures reach
// failureThreshold, the breaker trips to Open and records the trip time.
//
// Open: the tripped state. Requests are short-circuited with 503 without
// touching the dependency, giving the downstream time to recover and
// shielding it from load. Once cooldown has elapsed since the trip, the next
// request moves the breaker to Half-Open instead of short-circuiting.
//
// Half-Open: the probing state. Exactly one trial request is allowed through
// to test whether the dependency has recovered. If that probe succeeds, the
// breaker resets the failure counter and returns to Closed. If it fails, the
// breaker returns to Open and restarts the cooldown timer.
//
// Transitions:
//
//	Closed  --(failures >= threshold)--> Open
//	Open    --(cooldown elapsed)-------> Half-Open
//	HalfOpen--(probe success)----------> Closed
//	HalfOpen--(probe failure)----------> Open (cooldown timer reset)
//
// All state is guarded by a sync.Mutex so concurrent requests observe and
// mutate the breaker without data races.
package circuitbreaker

import (
	"net/http"
	"sync"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

// state enumerates the three circuit-breaker states.
type state int

const (
	stateClosed   state = iota // forwarding calls; counting failures
	stateOpen                  // short-circuiting; waiting out cooldown
	stateHalfOpen              // allowing a single probe call
)

// Breaker is a three-state circuit breaker wrapping a faildep.Dependency.
// The zero value is not usable; construct one with New.
type Breaker struct {
	mu               sync.Mutex
	state            state
	failureCount     int
	failureThreshold int
	cooldown         time.Duration
	lastOpenTime     time.Time
	dep              *faildep.Dependency
}

// New returns a Breaker that trips to Open after failureThreshold consecutive
// failures and stays Open for cooldown before allowing a Half-Open probe.
func New(dep *faildep.Dependency, failureThreshold int, cooldown time.Duration) *Breaker {
	return &Breaker{
		state:            stateClosed,
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		dep:              dep,
	}
}

// ServeHTTP drives the state machine for a single request.
//
// When Open, it short-circuits with 503 until cooldown elapses, at which point
// it promotes the breaker to Half-Open and lets this request act as the probe.
// The dependency is then called (in Closed or Half-Open). On failure it
// increments the counter and trips to Open once the threshold is reached (or
// immediately, when the failing call was the Half-Open probe), returning 503.
// On success it resets to Closed and returns 200 OK.
func (b *Breaker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	switch b.state {
	case stateOpen:
		if time.Since(b.lastOpenTime) < b.cooldown {
			b.mu.Unlock()
			http.Error(w, "circuit open", http.StatusServiceUnavailable)
			return
		}
		// Cooldown elapsed: promote to Half-Open and let this request probe.
		b.state = stateHalfOpen
	}
	b.mu.Unlock()

	err := b.dep.Call(r.Context())

	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.failureCount++
		// A failing probe re-opens immediately; in Closed we wait for the
		// threshold. Either way we (re)start the cooldown timer.
		if b.state == stateHalfOpen || b.failureCount >= b.failureThreshold {
			b.state = stateOpen
			b.lastOpenTime = time.Now()
		}
		http.Error(w, "dependency failed", http.StatusServiceUnavailable)
		return
	}

	// Success closes the breaker and clears the failure count.
	b.state = stateClosed
	b.failureCount = 0
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
