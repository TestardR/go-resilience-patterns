package circuitbreaker_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
	"github.com/TestardR/go-resilience-patterns/protect/circuitbreaker"
)

func serve(h http.Handler, r *http.Request) int {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Code
}

func TestBreaker_ServeHTTP(t *testing.T) {
	t.Run("closed_success_stays_200", func(t *testing.T) {
		dep := faildep.New(0.0, 0)
		b := circuitbreaker.New(dep, 2, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		for i := 0; i < 5; i++ {
			if code := serve(b, req); code != http.StatusOK {
				t.Fatalf("request %d: expected 200, got %d", i+1, code)
			}
		}
	})

	t.Run("trips_open_after_threshold", func(t *testing.T) {
		// 10ms latency makes a Closed-with-fail measurably slow, so we can
		// tell it apart from the instant Open short-circuit.
		dep := faildep.New(1.0, 10*time.Millisecond)
		b := circuitbreaker.New(dep, 2, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)

		// Requests 1 & 2: Closed-with-fail -> 503, each takes ~10ms.
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 1: expected 503, got %d", code)
		}
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 2: expected 503, got %d", code)
		}

		// Request 3: Open short-circuit -> 503 without touching dep, so it
		// must return well under the 10ms dep latency.
		start := time.Now()
		code := serve(b, req)
		elapsed := time.Since(start)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("request 3: expected 503, got %d", code)
		}
		if elapsed >= 8*time.Millisecond {
			t.Fatalf("expected Open short-circuit (< 8ms), got %v", elapsed)
		}
	})

	t.Run("recovers_via_half_open_to_closed", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		b := circuitbreaker.New(dep, 2, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)

		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 1: expected 503, got %d", code)
		}
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 2: expected 503, got %d", code)
		}
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 3 (open): expected 503, got %d", code)
		}

		// Sleep past cooldown, THEN mutate FailRate serially: no in-flight
		// Call, so the mutation cannot race with dep.Call.
		time.Sleep(150 * time.Millisecond)
		dep.FailRate = 0.0

		if code := serve(b, req); code != http.StatusOK {
			t.Fatalf("half-open probe: expected 200, got %d", code)
		}
		if code := serve(b, req); code != http.StatusOK {
			t.Fatalf("post-recovery request: expected 200, got %d", code)
		}
	})

	t.Run("half_open_probe_failure_reopens", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		b := circuitbreaker.New(dep, 2, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)

		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 1: expected 503, got %d", code)
		}
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 2: expected 503, got %d", code)
		}

		// Sleep past cooldown; probe still fails (FailRate 1.0) -> re-Open.
		time.Sleep(150 * time.Millisecond)
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("half-open probe: expected 503, got %d", code)
		}

		// Immediately after, the breaker is Open again: instant short-circuit.
		start := time.Now()
		code := serve(b, req)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("re-open request: expected 503, got %d", code)
		}
		if time.Since(start) >= 8*time.Millisecond {
			t.Fatalf("expected re-Open short-circuit, got slow response")
		}
	})

	t.Run("concurrent_half_open_single_probe", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		b := circuitbreaker.New(dep, 2, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)

		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 1: expected 503, got %d", code)
		}
		if code := serve(b, req); code != http.StatusServiceUnavailable {
			t.Fatalf("request 2: expected 503, got %d", code)
		}

		// Sleep past cooldown, then reconfigure dep to succeed but hold the
		// probe long enough that concurrent requests observe Half-Open. Both
		// mutations happen with no in-flight Call and before any goroutine
		// starts, so they cannot race.
		time.Sleep(150 * time.Millisecond)
		dep.FailRate = 0.0
		dep.Latency = 50 * time.Millisecond

		const N = 5
		var successes, failures int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				b.ServeHTTP(rec, req)
				if rec.Code == http.StatusOK {
					atomic.AddInt32(&successes, 1)
				} else {
					atomic.AddInt32(&failures, 1)
				}
			}()
		}
		close(start)
		wg.Wait()

		// The mutex serializes entry: exactly one request wins the Half-Open
		// probe slot and succeeds; the other N-1 see Half-Open and get 503.
		if got := atomic.LoadInt32(&successes); got != 1 {
			t.Fatalf("expected exactly 1 success (half-open probe), got %d", got)
		}
		if got := atomic.LoadInt32(&failures); got != N-1 {
			t.Fatalf("expected %d failures, got %d", N-1, got)
		}
	})
}
