package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func TestListBreaches(t *testing.T) {
	st := newFakeStore()
	h := newTestServer(t, st)
	do(t, h, http.MethodPost, "/v1/containers", validContainer)

	ended := testNow.Add(-time.Hour)
	st.breaches = []domain.Breach{
		{ID: 1, ContainerID: "CSQU3054383", Kind: domain.BreachTooWarm, StartedAt: testNow.Add(-30 * 24 * time.Hour), EndedAt: &ended},
		{ID: 2, ContainerID: "CSQU3054383", Kind: domain.BreachTooWarm, StartedAt: testNow.Add(-2 * time.Hour), EndedAt: &ended},
		{ID: 3, ContainerID: "OTHU0000001", Kind: domain.BreachTooCold, StartedAt: testNow.Add(-time.Hour)},
		{ID: 4, ContainerID: "CSQU3054383", Kind: domain.BreachTooCold, StartedAt: testNow.Add(-10 * time.Minute)},
	}

	get := func(path string) breachPage {
		t.Helper()
		rec := do(t, h, http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d %s", path, rec.Code, rec.Body)
		}
		var p breachPage
		if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ids := func(p breachPage) (out []int64) {
		for _, b := range p.Items {
			out = append(out, b.ID)
		}
		return out
	}

	// Default 7-day lookback hides breach 1; newest first.
	if got := ids(get("/v1/breaches")); len(got) != 3 || got[0] != 4 || got[2] != 2 {
		t.Fatalf("all = %v", got)
	}
	if got := ids(get("/v1/breaches?open=true")); len(got) != 2 || got[0] != 4 || got[1] != 3 {
		t.Fatalf("open = %v", got)
	}
	p := get("/v1/containers/CSQU3054383/breaches?limit=1")
	if got := ids(p); len(got) != 1 || got[0] != 4 || p.NextCursor != "4" {
		t.Fatalf("page 1 = %v cursor %q", got, p.NextCursor)
	}
	if got := ids(get("/v1/containers/CSQU3054383/breaches?limit=1&cursor=4")); len(got) != 1 || got[0] != 2 {
		t.Fatalf("page 2 = %v", got)
	}
	if rec := do(t, h, http.MethodGet, "/v1/breaches?cursor=abc", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor: %d", rec.Code)
	}
}
