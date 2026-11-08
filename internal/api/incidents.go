package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/incident"
)

// IncidentStore is the read side of the MongoDB incident log.
type IncidentStore interface {
	Get(ctx context.Context, breachID int64) (incident.Incident, error)
	List(ctx context.Context, f incident.Filter) ([]incident.Incident, error)
	Summarize(ctx context.Context, by incident.GroupBy, since time.Time, limit int) ([]incident.SummaryRow, error)
}

// WithIncidents enables the /v1/incidents endpoints. The incident log is a
// projection, so it is optional and its outage does not fail /readyz.
func WithIncidents(st IncidentStore) Option { return func(s *Server) { s.incidents = st } }

const defaultIncidentLookback = 30 * 24 * time.Hour

type incidentPage struct {
	Items      []incident.Incident `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

// listIncidents serves
// GET /v1/incidents?container=&owner=&status=open|closed&since=&limit=&cursor=
func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	since, err := parseTime("since", q.Get("since"), s.now().Add(-defaultIncidentLookback))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	f := incident.Filter{
		ContainerID: strings.ToUpper(q.Get("container")),
		Owner:       q.Get("owner"),
		Since:       since,
		Limit:       limit + 1,
	}
	switch st := incident.Status(q.Get("status")); st {
	case "", incident.StatusOpen, incident.StatusClosed:
		f.Status = st
	default:
		writeErr(w, r, &domain.ValidationError{Field: "status", Reason: "must be open or closed"})
		return
	}
	if c := q.Get("cursor"); c != "" {
		if f.Before, f.BeforeID, err = decodeIncidentCursor(c); err != nil {
			writeErr(w, r, err)
			return
		}
	}

	items, err := s.incidents.List(r.Context(), f)
	if err != nil {
		writeErr(w, r, err)
		return
	}
	page := incidentPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[limit-1]
		page.NextCursor = encodeIncidentCursor(last.StartedAt, last.BreachID)
	}
	if page.Items == nil {
		page.Items = []incident.Incident{}
	}
	writeJSON(w, http.StatusOK, page)
}

// getIncident serves GET /v1/incidents/{id}.
func (s *Server) getIncident(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, r, &domain.ValidationError{Field: "id", Reason: "must be a positive integer"})
		return
	}
	inc, err := s.incidents.Get(r.Context(), id)
	if errors.Is(err, incident.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, inc)
}

// summarizeIncidents serves
// GET /v1/incidents/summary?by=owner|commodity|container_id|kind&since=&limit=
func (s *Server) summarizeIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	by := incident.GroupBy(q.Get("by"))
	if by == "" {
		by = incident.ByOwner
	}
	if !by.Valid() {
		writeErr(w, r, &domain.ValidationError{Field: "by", Reason: "must be owner, commodity, container_id or kind"})
		return
	}
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	since, err := parseTime("since", q.Get("since"), s.now().Add(-defaultIncidentLookback))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	rows, err := s.incidents.Summarize(r.Context(), by, since, limit)
	if err != nil {
		writeErr(w, r, err)
		return
	}
	if rows == nil {
		rows = []incident.SummaryRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"by": by, "since": since, "groups": rows})
}

// The cursor is "<started_at unix nanos>.<breach id>" of the last item.
func encodeIncidentCursor(t time.Time, id int64) string {
	return fmt.Sprintf("%d.%d", t.UnixNano(), id)
}

func decodeIncidentCursor(c string) (time.Time, int64, error) {
	bad := &domain.ValidationError{Field: "cursor", Reason: "invalid"}
	ts, idStr, ok := strings.Cut(c, ".")
	if !ok {
		return time.Time{}, 0, bad
	}
	ns, err1 := strconv.ParseInt(ts, 10, 64)
	id, err2 := strconv.ParseInt(idStr, 10, 64)
	if err1 != nil || err2 != nil || id <= 0 {
		return time.Time{}, 0, bad
	}
	return time.Unix(0, ns).UTC(), id, nil
}
