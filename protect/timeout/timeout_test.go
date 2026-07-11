package timeout_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
	"github.com/TestardR/go-resilience-patterns/protect/timeout"
)

func TestTimeout(t *testing.T) {
	t.Run("fast_success_returns_200", func(t *testing.T) {
		dep := faildep.New(0.0, 10*time.Millisecond)
		h := timeout.New(dep, 100*time.Millisecond)
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

	t.Run("slow_dependency_times_out_504", func(t *testing.T) {
		dep := faildep.New(0.0, 200*time.Millisecond)
		h := timeout.New(dep, 50*time.Millisecond)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("expected 504, got %d", rec.Code)
		}
	})

	t.Run("dependency_error_returns_504", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		h := timeout.New(dep, 100*time.Millisecond)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("expected 504, got %d", rec.Code)
		}
	})

	t.Run("client_cancellation_returns_504", func(t *testing.T) {
		dep := faildep.New(0.0, 200*time.Millisecond)
		h := timeout.New(dep, 500*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		// Cancel the context before the handler can complete
		cancel()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("expected 504, got %d", rec.Code)
		}
	})
}
