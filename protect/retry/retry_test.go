package retry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
	"github.com/TestardR/go-resilience-patterns/protect/retry"
)

func TestHandler_ServeHTTP(t *testing.T) {
	t.Run("success_first_attempt", func(t *testing.T) {
		dep := faildep.New(0.0, 0)
		handler := retry.New(dep, 3, 1*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
		if body := rec.Body.String(); body != "ok" {
			t.Fatalf("expected body %q, got %q", "ok", body)
		}
	})

	t.Run("all_attempts_exhausted_502", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		handler := retry.New(dep, 3, 1*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected status 502, got %d", rec.Code)
		}
	})

	t.Run("misconfigured_maxAttempts_502", func(t *testing.T) {
		// Latency=100ms on dep proves dep was never invoked: if it had been,
		// the response would take >=100ms. The maxAttempts<=0 guard must
		// short-circuit before any dep.Call, so we expect an instant 502.
		dep := faildep.New(1.0, 100*time.Millisecond)
		handler := retry.New(dep, 0, 1*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		start := time.Now()
		handler.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected status 502, got %d", rec.Code)
		}
		if elapsed >= 50*time.Millisecond {
			t.Fatalf("expected elapsed < 50ms (dep must not be called), got %v", elapsed)
		}
	})

	t.Run("context_cancel_mid_backoff_502", func(t *testing.T) {
		// baseDelay=50ms gives a real backoff window that cancellation can
		// interrupt. dep is fast-failing (0 latency, 100%% failure) so the
		// handler is guaranteed to enter the time.After(sleep) select before
		// we cancel at ~10ms.
		dep := faildep.New(1.0, 0)
		handler := retry.New(dep, 5, 50*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		rec := httptest.NewRecorder()

		done := make(chan struct{})
		go func() {
			handler.ServeHTTP(rec, req)
			close(done)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		<-done

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected status 502, got %d", rec.Code)
		}
	})
}
