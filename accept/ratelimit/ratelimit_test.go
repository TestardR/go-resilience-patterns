package ratelimit_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/accept/ratelimit"
)

func TestRateLimit(t *testing.T) {
	t.Run("burst_up_to_capacity_allowed", func(t *testing.T) {
		l := ratelimit.New(1, 3)
		for i := 0; i < 3; i++ {
			if !l.Allow() {
				t.Fatalf("burst call %d: expected Allow()=true, got false", i+1)
			}
		}
		if l.Allow() {
			t.Fatal("4th call: expected Allow()=false, got true")
		}
	})

	t.Run("empty_bucket_rejects", func(t *testing.T) {
		l := ratelimit.New(1, 1)
		if !l.Allow() {
			t.Fatal("drain call: expected Allow()=true, got false")
		}
		if l.Allow() {
			t.Fatal("empty bucket: expected Allow()=false, got true")
		}
	})

	t.Run("refill_over_time", func(t *testing.T) {
		l := ratelimit.New(100, 1)
		if !l.Allow() {
			t.Fatal("drain call: expected Allow()=true, got false")
		}
		if l.Allow() {
			t.Fatal("post-drain: expected Allow()=false, got true")
		}
		time.Sleep(50 * time.Millisecond)
		if !l.Allow() {
			t.Fatal("after refill sleep: expected Allow()=true, got false")
		}
	})

	t.Run("zero_rate_never_admits", func(t *testing.T) {
		l := ratelimit.New(0, 0)
		if l.Allow() {
			t.Fatal("zero-rate limiter: expected Allow()=false, got true")
		}
	})

	t.Run("middleware_429_when_empty", func(t *testing.T) {
		okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		drained := ratelimit.New(1, 1)
		if !drained.Allow() {
			t.Fatal("drain call: expected Allow()=true, got false")
		}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		drained.Middleware(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("empty bucket middleware: expected 429, got %d", rec.Code)
		}

		fresh := ratelimit.New(1, 1)
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec2 := httptest.NewRecorder()
		fresh.Middleware(okHandler).ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Fatalf("fresh limiter middleware: expected 200, got %d", rec2.Code)
		}
	})
}
