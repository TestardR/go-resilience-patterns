package boundedqueue_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/accept/boundedqueue"
)

func serve(h http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestBoundedQueue(t *testing.T) {
	t.Run("enqueue_returns_202", func(t *testing.T) {
		q := boundedqueue.New(1, 1)
		rec := serve(q.Handler(func() {}))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", rec.Code)
		}
	})

	t.Run("worker_executes_work", func(t *testing.T) {
		q := boundedqueue.New(1, 1)
		done := make(chan struct{})
		rec := serve(q.Handler(func() { close(done) }))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", rec.Code)
		}
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("worker did not execute work within 200ms")
		}
	})

	t.Run("full_buffer_returns_503", func(t *testing.T) {
		q := boundedqueue.New(1, 1)
		release := make(chan struct{})

		workerStarted := make(chan struct{})
		blockUntilReleased := func() {
			close(workerStarted)
			<-release
		}
		if rec := serve(q.Handler(blockUntilReleased)); rec.Code != http.StatusAccepted {
			close(release)
			t.Fatalf("submit blocking work: expected 202, got %d", rec.Code)
		}

		select {
		case <-workerStarted:
		case <-time.After(200 * time.Millisecond):
			close(release)
			t.Fatal("worker never picked up blocking work")
		}

		fillBuffer := q.Handler(func() {})
		if rec := serve(fillBuffer); rec.Code != http.StatusAccepted {
			close(release)
			t.Fatalf("fill buffer: expected 202, got %d", rec.Code)
		}

		overflow := q.Handler(func() {})
		rec := serve(overflow)
		close(release)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("overflow submission: expected 503, got %d", rec.Code)
		}
	})

	t.Run("202_does_not_wait_on_work", func(t *testing.T) {
		q := boundedqueue.New(1, 1)
		gate := make(chan struct{})
		defer close(gate)

		h := q.Handler(func() { <-gate })

		start := time.Now()
		rec := serve(h)
		elapsed := time.Since(start)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", rec.Code)
		}
		if elapsed >= 50*time.Millisecond {
			t.Fatalf("handler blocked on work: elapsed=%v, want <50ms", elapsed)
		}
	})
}
