// Package timeout implements per-dependency timeout using context.WithTimeout.
//
// This is distinct from http.Server.WriteTimeout, which is a server-level
// write deadline. Here, each dependency call gets its own context deadline,
// propagating client cancellation while adding an explicit per-call limit.
package timeout

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

// Handler wraps a faildep.Dependency and enforces a per-call context deadline.
type Handler struct {
	dep     *faildep.Dependency
	timeout time.Duration
}

// New returns a Handler that will enforce timeout on every dependency call.
func New(dep *faildep.Dependency, timeout time.Duration) *Handler {
	return &Handler{dep: dep, timeout: timeout}
}

// ServeHTTP derives a timeout context from the incoming request context,
// calls the dependency, and returns 504 Gateway Timeout on deadline exceeded
// or any other error, and 200 OK on success.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	if err := h.dep.Call(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "dependency timed out", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "dependency error", http.StatusGatewayTimeout)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
