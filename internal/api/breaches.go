package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

const defaultBreachLookback = 7 * 24 * time.Hour

type breachPage struct {
	Items      []domain.Breach `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// listBreaches serves GET /v1/breaches?open=true&since=&limit=&cursor=
// newest first, across all containers.
func (s *Server) listBreaches(w http.ResponseWriter, r *http.Request) {
	s.serveBreaches(w, r, "")
}

// listContainerBreaches serves GET /v1/containers/{id}/breaches.
func (s *Server) listContainerBreaches(w http.ResponseWriter, r *http.Request) {
	id := strings.ToUpper(r.PathValue("id"))
	if _, err := s.store.GetContainer(r.Context(), id); err != nil {
		writeErr(w, r, err)
		return
	}
	s.serveBreaches(w, r, id)
}

func (s *Server) serveBreaches(w http.ResponseWriter, r *http.Request, containerID string) {
	q := r.URL.Query()
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	since, err := parseTime("since", q.Get("since"), s.now().Add(-defaultBreachLookback))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	var before int64
	if c := q.Get("cursor"); c != "" {
		before, err = strconv.ParseInt(c, 10, 64)
		if err != nil || before <= 0 {
			writeErr(w, r, &domain.ValidationError{Field: "cursor", Reason: "invalid"})
			return
		}
	}
	openOnly := q.Get("open") == "true"

	items, err := s.store.ListBreaches(r.Context(), store.BreachFilter{
		ContainerID: containerID, OpenOnly: openOnly, Since: since, BeforeID: before, Limit: limit + 1,
	})
	if err != nil {
		writeErr(w, r, err)
		return
	}
	page := breachPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = strconv.FormatInt(page.Items[limit-1].ID, 10)
	}
	if page.Items == nil {
		page.Items = []domain.Breach{}
	}
	writeJSON(w, http.StatusOK, page)
}
