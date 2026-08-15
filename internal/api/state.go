package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// staleAfter marks a container as stale when it has not reported recently.
// Reefer controllers report every few seconds to minutes, so 15 minutes of
// silence usually means a power or connectivity problem worth surfacing.
const staleAfter = 15 * time.Minute

type stateResponse struct {
	ContainerID string             `json:"container_id"`
	Latest      domain.Reading     `json:"latest"`
	Breach      domain.BreachState `json:"breach"`
	InBand      bool               `json:"in_band"`
	Stale       bool               `json:"stale"`
	AgeSeconds  float64            `json:"age_seconds"`
}

// getState serves GET /v1/containers/{id}/state from Redis only.
func (s *Server) getState(w http.ResponseWriter, r *http.Request) {
	id := strings.ToUpper(r.PathValue("id"))
	st, err := s.cache.Latest(r.Context(), id)
	if errors.Is(err, cache.ErrMiss) {
		writeError(w, r, http.StatusNotFound, "no recent telemetry for container")
		return
	}
	if err != nil {
		writeErr(w, r, err)
		return
	}
	age := s.now().Sub(st.Latest.RecordedAt)
	writeJSON(w, http.StatusOK, stateResponse{
		ContainerID: id,
		Latest:      st.Latest,
		Breach:      st.Breach,
		InBand:      !st.Breach.Open,
		Stale:       age > staleAfter,
		AgeSeconds:  age.Seconds(),
	})
}
