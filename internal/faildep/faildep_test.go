package faildep_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

func TestDependency_Call(t *testing.T) {
	t.Run("fail_rate_0_always_succeeds", func(t *testing.T) {
		dep := faildep.New(0.0, 0)
		ctx := context.Background()
		for i := 0; i < 50; i++ {
			if err := dep.Call(ctx); err != nil {
				t.Fatalf("expected nil, got %v on iteration %d", err, i)
			}
		}
	})

	t.Run("fail_rate_1_always_fails", func(t *testing.T) {
		dep := faildep.New(1.0, 0)
		ctx := context.Background()
		for i := 0; i < 50; i++ {
			err := dep.Call(ctx)
			if !errors.Is(err, faildep.ErrFailed) {
				t.Fatalf("expected ErrFailed, got %v on iteration %d", err, i)
			}
		}
	})

	t.Run("cancelled_context_returns_ctx_err", func(t *testing.T) {
		dep := faildep.New(0.0, 50*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := dep.Call(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("deadline_shorter_than_latency", func(t *testing.T) {
		dep := faildep.New(0.0, 100*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := dep.Call(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
	})

	t.Run("latency_respected_on_success", func(t *testing.T) {
		dep := faildep.New(0.0, 50*time.Millisecond)
		ctx := context.Background()
		start := time.Now()
		if err := dep.Call(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed < 45*time.Millisecond {
			t.Fatalf("expected elapsed >= 45ms, got %v", elapsed)
		}
	})
}
