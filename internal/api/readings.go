package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

const (
	defaultHistoryWindow = 24 * time.Hour
	// maxHistoryWindow keeps a single request from scanning months of data;
	// bulk exports should come from the S3 archive instead.
	maxHistoryWindow = 31 * 24 * time.Hour
)

type readingPage struct {
	ContainerID string           `json:"container_id"`
	From        time.Time        `json:"from"`
	To          time.Time        `json:"to"`
	Items       []domain.Reading `json:"items"`
	NextCursor  string           `json:"next_cursor,omitempty"`
}

// listReadings serves GET /v1/containers/{id}/readings?from=&to=&limit=&cursor=
// Timestamps are RFC 3339. The window defaults to the last 24 hours.
func (s *Server) listReadings(w http.ResponseWriter, r *http.Request) {
	id := strings.ToUpper(r.PathValue("id"))
	q := r.URL.Query()

	to, err := parseTime("to", q.Get("to"), s.now())
	if err != nil {
		writeErr(w, r, err)
		return
	}
	from, err := parseTime("from", q.Get("from"), to.Add(-defaultHistoryWindow))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	if !from.Before(to) {
		writeErr(w, r, &domain.ValidationError{Field: "from", Reason: "must be before to"})
		return
	}
	if to.Sub(from) > maxHistoryWindow {
		writeErr(w, r, &domain.ValidationError{Field: "window", Reason: "must not exceed 31 days"})
		return
	}
	after, err := parseTime("cursor", q.Get("cursor"), time.Time{})
	if err != nil {
		writeErr(w, r, err)
		return
	}
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeErr(w, r, err)
		return
	}

	// 404 for unknown containers rather than an ambiguous empty list.
	if _, err := s.store.GetContainer(r.Context(), id); err != nil {
		writeErr(w, r, err)
		return
	}

	items, err := s.store.ReadingsInRange(r.Context(), store.ReadingRange{
		ContainerID: id, From: from, To: to, After: after, Limit: limit + 1,
	})
	if err != nil {
		writeErr(w, r, err)
		return
	}
	page := readingPage{ContainerID: id, From: from, To: to, Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = page.Items[limit-1].RecordedAt.Format(time.RFC3339Nano)
	}
	if page.Items == nil {
		page.Items = []domain.Reading{}
	}
	writeJSON(w, http.StatusOK, page)
}

func parseTime(field, raw string, def time.Time) (time.Time, error) {
	if raw == "" {
		return def, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, &domain.ValidationError{Field: field, Reason: "must be an RFC 3339 timestamp"}
	}
	return t.UTC(), nil
}
