package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

type registerContainerRequest struct {
	ID        string   `json:"id"`
	Owner     string   `json:"owner"`
	Commodity string   `json:"commodity"`
	SetpointC *float64 `json:"setpoint_c"`
	MinTempC  *float64 `json:"min_temp_c"`
	MaxTempC  *float64 `json:"max_temp_c"`
}

func (s *Server) createContainer(w http.ResponseWriter, r *http.Request) {
	var req registerContainerRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeErr(w, r, err)
		return
	}
	// Pointers distinguish "0 °C" from "field missing".
	required := []struct {
		name string
		v    *float64
	}{{"setpoint_c", req.SetpointC}, {"min_temp_c", req.MinTempC}, {"max_temp_c", req.MaxTempC}}
	for _, f := range required {
		if f.v == nil {
			writeErr(w, r, &domain.ValidationError{Field: f.name, Reason: "required"})
			return
		}
	}
	c := domain.Container{
		ID: req.ID, Owner: req.Owner, Commodity: req.Commodity,
		SetpointC: *req.SetpointC, MinTempC: *req.MinTempC, MaxTempC: *req.MaxTempC,
	}
	c.Normalize()
	if err := c.Validate(); err != nil {
		writeErr(w, r, err)
		return
	}
	created, err := s.store.CreateContainer(r.Context(), c)
	if err != nil {
		writeErr(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/containers/"+created.ID)
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) getContainer(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.GetContainer(r.Context(), strings.ToUpper(r.PathValue("id")))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

type containerPage struct {
	Items      []domain.Container `json:"items"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

func (s *Server) listContainers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	// Fetch one extra row to know whether another page exists.
	items, err := s.store.ListContainers(r.Context(), store.ContainerFilter{
		Owner: q.Get("owner"), After: q.Get("cursor"), Limit: limit + 1,
	})
	if err != nil {
		writeErr(w, r, err)
		return
	}
	page := containerPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = page.Items[limit-1].ID
	}
	if page.Items == nil {
		page.Items = []domain.Container{}
	}
	writeJSON(w, http.StatusOK, page)
}

func parseLimit(raw string) (int, error) {
	if raw == "" {
		return defaultPageSize, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n > maxPageSize {
		return 0, &domain.ValidationError{Field: "limit", Reason: "must be an integer in [1, 1000]"}
	}
	return n, nil
}
