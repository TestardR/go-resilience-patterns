package degradation_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/TestardR/go-resilience-patterns/accept/degradation"
	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

func TestDegradation(t *testing.T) {
	t.Run("primary_success_serves_live", func(t *testing.T) {
		dep := faildep.New(0.0, 0)
		h := degradation.New(dep)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if got := rec.Body.String(); got != `{"data":"live"}` {
			t.Fatalf("expected live body, got %q", got)
		}
		if rec.Header().Get("X-Degraded") != "" {
			t.Fatal("X-Degraded header must be absent on success")
		}
	})

	t.Run("primary_failure_serves_cached", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		h := degradation.New(dep)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if got := rec.Header().Get("X-Degraded"); got != "true" {
			t.Fatalf("expected X-Degraded: true, got %q", got)
		}
		if got := rec.Body.String(); got != `{"data":"cached","degraded":true}` {
			t.Fatalf("expected cached body, got %q", got)
		}
	})
}
