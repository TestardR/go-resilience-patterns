// Package degradation implements graceful degradation for net/http servers.
// Graceful degradation serves a reduced-quality (fallback / cached) response
// with a 200 status when the primary dependency fails, so clients still get
// something useful. This is distinct from load shedding, which rejects excess
// requests outright with a 503 Service Unavailable.
package degradation

import (
	"net/http"

	"github.com/TestardR/go-resilience-patterns/internal/faildep"
)

// Handler is an http.Handler that calls a primary dependency and falls back
// to a cached/default response when the dependency call fails. The fallback
// response is served with HTTP 200 and an X-Degraded: true header so clients
// can detect that the payload is a degraded version.
type Handler struct {
	dep *faildep.Dependency
}

// New returns a Handler that guards calls to dep with a graceful-degradation
// fallback.
func New(dep *faildep.Dependency) *Handler {
	return &Handler{dep: dep}
}

// ServeHTTP invokes the primary dependency. On success it writes the live
// response; on any error it writes the cached fallback response with
// X-Degraded: true. Both paths return HTTP 200 — degradation never fails
// the request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.dep.Call(r.Context()); err != nil {
		w.Header().Set("X-Degraded", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"cached","degraded":true}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"data":"live"}`))
}
