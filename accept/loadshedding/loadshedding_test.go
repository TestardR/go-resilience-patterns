package loadshedding_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/TestardR/go-resilience-patterns/accept/loadshedding"
)

func TestLoadShedding(t *testing.T) {
	t.Run("under_limit_passes", func(t *testing.T) {
		s := loadshedding.New(5)
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		s.Middleware(next).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})

	t.Run("at_limit_all_pass", func(t *testing.T) {
		const n = 3
		s := loadshedding.New(n)
		gate := make(chan struct{})
		var arrived sync.WaitGroup
		arrived.Add(n)

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			arrived.Done()
			<-gate
			w.WriteHeader(http.StatusOK)
		})
		handler := s.Middleware(next)

		recs := make([]*httptest.ResponseRecorder, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			i := i
			recs[i] = httptest.NewRecorder()
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				handler.ServeHTTP(recs[i], req)
			}()
		}

		arrived.Wait()
		close(gate)
		wg.Wait()

		for i, rec := range recs {
			if rec.Code != http.StatusOK {
				t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
			}
		}
	})

	t.Run("over_limit_sheds_503", func(t *testing.T) {
		const maxInFlight = 3
		const total = 6
		s := loadshedding.New(maxInFlight)
		gate := make(chan struct{})
		decided := make(chan struct{}, total)

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			decided <- struct{}{}
			<-gate
			w.WriteHeader(http.StatusOK)
		})
		handler := s.Middleware(next)

		var successes, sheds atomic.Int32
		var wg sync.WaitGroup
		wg.Add(total)
		for i := 0; i < total; i++ {
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				switch rec.Code {
				case http.StatusOK:
					successes.Add(1)
				case http.StatusServiceUnavailable:
					sheds.Add(1)
					decided <- struct{}{}
				}
			}()
		}

		for i := 0; i < total; i++ {
			<-decided
		}
		close(gate)
		wg.Wait()

		got := successes.Load()
		gotSheds := sheds.Load()
		if got+gotSheds != int32(total) {
			t.Fatalf("expected %d total responses, got successes=%d sheds=%d", total, got, gotSheds)
		}
		if got > int32(maxInFlight) {
			t.Fatalf("expected successes <= %d, got %d", maxInFlight, got)
		}
		if gotSheds < 1 {
			t.Fatalf("expected sheds >= 1, got %d", gotSheds)
		}
	})

	t.Run("counter_releases_after_burst", func(t *testing.T) {
		const maxInFlight = 3
		const total = 6
		s := loadshedding.New(maxInFlight)
		gate := make(chan struct{})

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-gate
			w.WriteHeader(http.StatusOK)
		})
		handler := s.Middleware(next)

		var wg sync.WaitGroup
		wg.Add(total)
		for i := 0; i < total; i++ {
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
			}()
		}

		close(gate)
		wg.Wait()

		fresh := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		s.Middleware(fresh).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("post-burst request: expected 200, got %d", rec.Code)
		}
	})
}
