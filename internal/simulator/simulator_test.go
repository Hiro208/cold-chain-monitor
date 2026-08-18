package simulator

import (
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func TestFleetIDsAreValidAndUnique(t *testing.T) {
	f := NewFleet(2000, 0, 1)
	seen := map[string]bool{}
	for _, u := range f.Units {
		if err := u.Container.Validate(); err != nil {
			t.Fatalf("%s: %v", u.Container.ID, err)
		}
		if seen[u.Container.ID] {
			t.Fatalf("duplicate id %s", u.Container.ID)
		}
		seen[u.Container.ID] = true
	}
}

func TestHealthyFleetStaysInBand(t *testing.T) {
	f := NewFleet(200, 0, 42)
	t0 := time.Now()
	breaches := 0
	for tick := 0; tick < 200; tick++ {
		for i := range f.Units {
			r := f.Step(i, t0.Add(time.Duration(tick)*time.Second))
			if domain.BandOf(f.Units[i].Container, 0).Classify(r.TemperatureC) != "" && !r.DoorOpen {
				breaches++
			}
		}
	}
	// Noise alone may occasionally graze a limit, but it should be rare.
	if rate := float64(breaches) / (200 * 200); rate > 0.01 {
		t.Fatalf("healthy fleet breach rate %.3f too high", rate)
	}
}

func TestFaultsProduceBreaches(t *testing.T) {
	f := NewFleet(100, 0.01, 7)
	t0 := time.Now()
	opened := 0
	states := make([]domain.BreachState, len(f.Units))
	for tick := 0; tick < 300; tick++ {
		for i, u := range f.Units {
			r := f.Step(i, t0.Add(time.Duration(tick)*time.Second))
			var tr domain.TransitionType
			states[i], tr = domain.Evaluate(domain.BandOf(u.Container, 0.5), states[i], r)
			if tr == domain.TransitionOpened {
				opened++
			}
		}
	}
	if opened == 0 {
		t.Fatal("expected injected faults to open breaches")
	}
}
