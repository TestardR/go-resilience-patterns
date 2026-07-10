// Package faildep simulates a slow and intermittently failing downstream
// dependency for use by the protect/* pattern packages. It is intentionally
// simple: no resilience logic lives here — faildep is the thing being
// protected against.
package faildep

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrFailed is returned when the simulated dependency fails.
var ErrFailed = errors.New("faildep: dependency call failed")

// Dependency simulates a downstream service that can be slow and unreliable.
type Dependency struct {
	FailRate float64       // probability of failure [0.0, 1.0]
	Latency  time.Duration // simulated response time
	rng      *rand.Rand
}

// New creates a Dependency with the given failure rate and latency.
func New(failRate float64, latency time.Duration) *Dependency {
	return &Dependency{
		FailRate: failRate,
		Latency:  latency,
		rng:      rand.New(rand.NewSource(42)),
	}
}

// Call simulates a dependency call. It respects ctx cancellation/deadline
// (so the timeout pattern can cancel it) and returns ErrFailed at FailRate
// probability (so retry and circuit-breaker patterns have something to react to).
func (d *Dependency) Call(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.Latency):
	}
	if d.rng.Float64() < d.FailRate {
		return ErrFailed
	}
	return nil
}
