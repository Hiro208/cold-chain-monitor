package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func TestGetState(t *testing.T) {
	fc := newFakeCache()
	fc.states["CSQU3054383"] = cache.State{
		Latest: domain.Reading{ContainerID: "CSQU3054383", RecordedAt: testNow.Add(-20 * time.Minute), TemperatureC: 9.5},
		Breach: domain.BreachState{Open: true, Kind: domain.BreachTooWarm, PeakTempC: 9.5},
	}
	h := newTestServerWithCache(t, newFakeStore(), fc)

	rec := do(t, h, http.MethodGet, "/v1/containers/csqu3054383/state", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var got stateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.InBand || !got.Stale || got.Breach.Kind != domain.BreachTooWarm || got.AgeSeconds != 1200 {
		t.Fatalf("unexpected state %+v", got)
	}

	if rec := do(t, h, http.MethodGet, "/v1/containers/ABCU0000000/state", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cache miss: status %d", rec.Code)
	}
}
