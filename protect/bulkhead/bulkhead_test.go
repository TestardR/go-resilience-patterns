package bulkhead_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
	"github.com/TestardR/go-resilience-patterns/protect/bulkhead"
)

func TestBulkhead(t *testing.T) {
	t.Run("unknown_partition_503", func(t *testing.T) {
		b := bulkhead.New(map[string]int{"a": 1})
		dep := faildep.New(0.0, 0)
		h := b.Handler("missing", dep)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d", rec.Code)
		}
	})

	t.Run("success_returns_200", func(t *testing.T) {
		b := bulkhead.New(map[string]int{"a": 1})
		dep := faildep.New(0.0, 0)
		h := b.Handler("a", dep)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if rec.Body.String() != "ok" {
			t.Fatalf("expected body 'ok', got %q", rec.Body.String())
		}
	})

	t.Run("dependency_error_502", func(t *testing.T) {
		b := bulkhead.New(map[string]int{"a": 1})
		dep := faildep.New(1.0, 0)
		h := b.Handler("a", dep)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d", rec.Code)
		}
	})

	t.Run("zero_capacity_always_503", func(t *testing.T) {
		// cap=0 → unbuffered channel → non-blocking acquire always fails.
		bZero := bulkhead.New(map[string]int{"z": 0})
		dep := faildep.New(0.0, 0)
		hZero := bZero.Handler("z", dep)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		hZero.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for zero cap, got %d", rec.Code)
		}

		// Negative cap is clamped to 0, same behavior.
		bNeg := bulkhead.New(map[string]int{"neg": -1})
		hNeg := bNeg.Handler("neg", dep)

		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec2 := httptest.NewRecorder()
		hNeg.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for negative cap, got %d", rec2.Code)
		}
	})

	t.Run("full_partition_503", func(t *testing.T) {
		// Latency=100ms so request 1 holds the sole slot long enough for
		// request 2 to observe partition "a" as full.
		b := bulkhead.New(map[string]int{"a": 1})
		dep := faildep.New(0.0, 100*time.Millisecond)
		h := b.Handler("a", dep)

		var wg sync.WaitGroup
		wg.Add(1)
		rec1 := httptest.NewRecorder()
		go func() {
			defer wg.Done()
			req1 := httptest.NewRequest(http.MethodGet, "/", nil)
			h.ServeHTTP(rec1, req1)
		}()

		// Give req1 time to acquire the slot before firing req2.
		time.Sleep(10 * time.Millisecond)

		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 (partition full), got %d", rec2.Code)
		}

		wg.Wait()

		if rec1.Code != http.StatusOK {
			t.Fatalf("expected req1 to succeed with 200, got %d", rec1.Code)
		}
	})

	t.Run("partition_isolation", func(t *testing.T) {
		// Partition "a" full must not affect partition "b".
		b := bulkhead.New(map[string]int{"a": 1, "b": 1})
		dep := faildep.New(0.0, 100*time.Millisecond)
		hA := b.Handler("a", dep)
		hB := b.Handler("b", dep)

		var wg sync.WaitGroup
		wg.Add(1)
		recA := httptest.NewRecorder()
		go func() {
			defer wg.Done()
			reqA := httptest.NewRequest(http.MethodGet, "/", nil)
			hA.ServeHTTP(recA, reqA)
		}()

		// Ensure the "a" partition slot is held before firing on "b".
		time.Sleep(10 * time.Millisecond)

		reqB := httptest.NewRequest(http.MethodGet, "/", nil)
		recB := httptest.NewRecorder()
		hB.ServeHTTP(recB, reqB)

		if recB.Code != http.StatusOK {
			t.Fatalf("expected partition b to succeed with 200, got %d", recB.Code)
		}

		wg.Wait()

		if recA.Code != http.StatusOK {
			t.Fatalf("expected partition a to succeed with 200, got %d", recA.Code)
		}
	})
}
