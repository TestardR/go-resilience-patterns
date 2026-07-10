// Package loadshedding implements load shedding middleware for net/http servers.
// Load shedding rejects excess requests with a 503 when the server is overloaded.
// This is distinct from graceful degradation, which serves a reduced (fallback)
// response with a 200 instead of rejecting the request entirely.
package loadshedding

import (
	"net/http"
	"sync/atomic"
)

// Shedder tracks in-flight requests and rejects new ones once MaxInFlight is exceeded.
type Shedder struct {
	MaxInFlight int64
	inFlight    atomic.Int64
}

// New returns a Shedder that allows at most maxInFlight concurrent requests.
func New(maxInFlight int64) *Shedder {
	return &Shedder{MaxInFlight: maxInFlight}
}

// Middleware wraps next and returns 503 Service Unavailable when the number of
// concurrent in-flight requests exceeds MaxInFlight.
func (s *Shedder) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := s.inFlight.Add(1)
		defer s.inFlight.Add(-1)
		if current > s.MaxInFlight {
			http.Error(w, "server overloaded", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
