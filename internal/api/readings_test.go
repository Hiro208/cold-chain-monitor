package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func seedReadings(st *fakeStore, id string, start time.Time, n int, step time.Duration) {
	for i := 0; i < n; i++ {
		st.readings = append(st.readings, domain.Reading{
			ContainerID: id, RecordedAt: start.Add(time.Duration(i) * step), TemperatureC: 4, HumidityPct: 70,
		})
	}
}

func TestListReadingsWindowAndPaging(t *testing.T) {
	st := newFakeStore()
	h := newTestServer(t, st)
	do(t, h, http.MethodPost, "/v1/containers", validContainer)
	// 10 readings, 1 minute apart, ending 1 minute before testNow.
	seedReadings(st, "CSQU3054383", testNow.Add(-10*time.Minute), 10, time.Minute)
	// One reading outside the default 24h window.
	seedReadings(st, "CSQU3054383", testNow.Add(-48*time.Hour), 1, 0)

	var all []domain.Reading
	cursor := ""
	for i := 0; i < 10; i++ {
		v := url.Values{"limit": {"4"}}
		if cursor != "" {
			v.Set("cursor", cursor)
		}
		rec := do(t, h, http.MethodGet, "/v1/containers/CSQU3054383/readings?"+v.Encode(), "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d %s", rec.Code, rec.Body)
		}
		var page readingPage
		if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Items...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(all) != 10 {
		t.Fatalf("got %d readings, want 10", len(all))
	}
	for i := 1; i < len(all); i++ {
		if !all[i].RecordedAt.After(all[i-1].RecordedAt) {
			t.Fatalf("readings not strictly ascending at %d", i)
		}
	}
}

func TestListReadingsValidation(t *testing.T) {
	h := newTestServer(t, newFakeStore())
	do(t, h, http.MethodPost, "/v1/containers", validContainer)

	cases := map[string]string{
		"bad from":       "from=yesterday",
		"inverted":       "from=2026-03-02T00:00:00Z&to=2026-03-01T00:00:00Z",
		"window too big": "from=2026-01-01T00:00:00Z&to=2026-03-01T00:00:00Z",
		"bad limit":      "limit=5000",
	}
	for name, qs := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := do(t, h, http.MethodGet, "/v1/containers/CSQU3054383/readings?"+qs, ""); rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d", rec.Code)
			}
		})
	}
	if rec := do(t, h, http.MethodGet, "/v1/containers/ABCU0000000/readings", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown container: status %d", rec.Code)
	}
}
