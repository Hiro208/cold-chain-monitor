package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/incident"
)

type fakeIncidents struct {
	items   []incident.Incident
	lastF   incident.Filter
	lastBy  incident.GroupBy
	summary []incident.SummaryRow
}

func (f *fakeIncidents) Get(_ context.Context, id int64) (incident.Incident, error) {
	for _, it := range f.items {
		if it.BreachID == id {
			return it, nil
		}
	}
	return incident.Incident{}, incident.ErrNotFound
}

// List mimics the Mongo query: items are kept newest first.
func (f *fakeIncidents) List(_ context.Context, flt incident.Filter) ([]incident.Incident, error) {
	f.lastF = flt
	var out []incident.Incident
	for _, it := range f.items {
		if flt.Status != "" && it.Status != flt.Status {
			continue
		}
		if !flt.Before.IsZero() && !(it.StartedAt.Before(flt.Before) || (it.StartedAt.Equal(flt.Before) && it.BreachID < flt.BeforeID)) {
			continue
		}
		out = append(out, it)
		if len(out) == flt.Limit {
			break
		}
	}
	return out, nil
}

func (f *fakeIncidents) Summarize(_ context.Context, by incident.GroupBy, _ time.Time, _ int) ([]incident.SummaryRow, error) {
	f.lastBy = by
	return f.summary, nil
}

func TestIncidentEndpoints(t *testing.T) {
	fi := &fakeIncidents{
		items: []incident.Incident{
			{BreachID: 3, ContainerID: "CSQU3054383", Status: incident.StatusOpen, StartedAt: testNow.Add(-time.Minute)},
			{BreachID: 2, ContainerID: "CSQU3054383", Status: incident.StatusClosed, StartedAt: testNow.Add(-time.Hour)},
			{BreachID: 1, ContainerID: "OTHU0000001", Status: incident.StatusClosed, StartedAt: testNow.Add(-2 * time.Hour)},
		},
		summary: []incident.SummaryRow{{Key: "acme", Incidents: 3, Open: 1}},
	}
	h := newTestServer(t, newFakeStore(), WithIncidents(fi))

	get := func(path string, want int) []byte {
		t.Helper()
		rec := do(t, h, http.MethodGet, path, "")
		if rec.Code != want {
			t.Fatalf("%s: status %d, want %d: %s", path, rec.Code, want, rec.Body)
		}
		return rec.Body.Bytes()
	}

	var p incidentPage
	if err := json.Unmarshal(get("/v1/incidents?limit=2&container=csqu3054383", 200), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 2 || p.Items[0].BreachID != 3 || p.NextCursor == "" {
		t.Fatalf("page 1 = %+v", p)
	}
	if fi.lastF.ContainerID != "CSQU3054383" || fi.lastF.Limit != 3 {
		t.Fatalf("filter = %+v", fi.lastF)
	}
	var p2 incidentPage
	if err := json.Unmarshal(get("/v1/incidents?limit=2&cursor="+p.NextCursor, 200), &p2); err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 1 || p2.Items[0].BreachID != 1 || p2.NextCursor != "" {
		t.Fatalf("page 2 = %+v", p2)
	}

	get("/v1/incidents?status=open", 200)
	if fi.lastF.Status != incident.StatusOpen {
		t.Fatalf("status filter = %q", fi.lastF.Status)
	}
	get("/v1/incidents?status=bogus", 400)
	get("/v1/incidents?cursor=nope", 400)

	var one incident.Incident
	if err := json.Unmarshal(get("/v1/incidents/2", 200), &one); err != nil || one.BreachID != 2 {
		t.Fatalf("get = %+v %v", one, err)
	}
	get("/v1/incidents/99", 404)
	get("/v1/incidents/abc", 400)

	body := get("/v1/incidents/summary?by=commodity", 200)
	if fi.lastBy != incident.ByCommodity {
		t.Fatalf("by = %q", fi.lastBy)
	}
	var sum struct {
		Groups []incident.SummaryRow `json:"groups"`
	}
	if err := json.Unmarshal(body, &sum); err != nil || len(sum.Groups) != 1 || sum.Groups[0].Open != 1 {
		t.Fatalf("summary = %s", body)
	}
	get("/v1/incidents/summary?by=$where", 400)
}

func TestIncidentEndpointsDisabledWithoutMongo(t *testing.T) {
	h := newTestServer(t, newFakeStore())
	if rec := do(t, h, http.MethodGet, "/v1/incidents", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 when MONGODB_URI is unset", rec.Code)
	}
}
