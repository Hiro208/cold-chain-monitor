package stream

import (
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func TestReadingRoundTrip(t *testing.T) {
	in := domain.Reading{
		ContainerID:  "CSQU3054383",
		RecordedAt:   time.Date(2026, 5, 1, 10, 0, 0, 123456000, time.UTC),
		TemperatureC: -18.25,
		HumidityPct:  42,
		DoorOpen:     true,
	}
	b, err := EncodeReading(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeReading(b)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	if _, err := DecodeReading([]byte(`{"v":2,"container_id":"X"}`)); err == nil {
		t.Fatal("expected error for v2")
	}
	if _, err := DecodeReading([]byte(`not json`)); err == nil {
		t.Fatal("expected error for garbage")
	}
	if _, err := DecodeBreachEvent([]byte(`{"type":"breach.opened"}`)); err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestBreachEventRoundTrip(t *testing.T) {
	in := BreachEvent{
		Type:       BreachOpened,
		Breach:     domain.Breach{ID: 7, ContainerID: "CSQU3054383", Kind: domain.BreachTooWarm, PeakTempC: 9.1, LimitC: 8},
		Owner:      "acme",
		DetectedAt: time.Date(2026, 5, 1, 10, 0, 1, 0, time.UTC),
		LatencyMs:  840,
	}
	b, err := EncodeBreachEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeBreachEvent(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != in.Type || out.Breach.ID != 7 || out.LatencyMs != 840 || out.V != SchemaVersion {
		t.Fatalf("unexpected %+v", out)
	}
}
