package domain

import (
	"math"
	"testing"
	"time"
)

func TestValidateContainerID(t *testing.T) {
	tests := []struct {
		id      string
		wantErr bool
	}{
		{"CSQU3054383", false}, // canonical ISO 6346 example
		{"CSQU3054384", true},  // wrong check digit
		{"CSQX3054383", true},  // bad equipment category
		{"CSQU305438", true},   // too short
		{"C5QU3054383", true},  // digit in owner code
		{"CSQU30543A3", true},  // letter in serial
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			err := ValidateContainerID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateContainerID(%q) err = %v, wantErr %v", tt.id, err, tt.wantErr)
			}
			if err != nil && !IsValidationError(err) {
				t.Fatalf("expected ValidationError, got %T", err)
			}
		})
	}
}

func TestMakeContainerIDRoundTrip(t *testing.T) {
	for serial := 0; serial < 5000; serial += 7 {
		id := MakeContainerID("sim", serial)
		if err := ValidateContainerID(id); err != nil {
			t.Fatalf("MakeContainerID(%d) = %q is invalid: %v", serial, id, err)
		}
	}
}

func TestContainerValidate(t *testing.T) {
	valid := Container{ID: "CSQU3054383", Owner: "acme", SetpointC: 4, MinTempC: 2, MaxTempC: 8}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid container rejected: %v", err)
	}

	cases := map[string]func(c *Container){
		"missing owner":      func(c *Container) { c.Owner = "" },
		"inverted band":      func(c *Container) { c.MinTempC, c.MaxTempC = 8, 2 },
		"setpoint off band":  func(c *Container) { c.SetpointC = 12 },
		"unsupported range":  func(c *Container) { c.MinTempC = -60 },
		"invalid identifier": func(c *Container) { c.ID = "nope" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid
			mutate(&c)
			if err := c.Validate(); !IsValidationError(err) {
				t.Fatalf("expected validation error, got %v", err)
			}
		})
	}
}

func TestReadingValidate(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ok := Reading{ContainerID: "CSQU3054383", RecordedAt: now, TemperatureC: 4, HumidityPct: 80}
	if err := ok.Validate(now); err != nil {
		t.Fatalf("valid reading rejected: %v", err)
	}
	bad := []Reading{
		{RecordedAt: now},
		{ContainerID: "X"},
		{ContainerID: "X", RecordedAt: now.Add(time.Hour)},
		{ContainerID: "X", RecordedAt: now, TemperatureC: math.NaN()},
		{ContainerID: "X", RecordedAt: now, HumidityPct: 101},
	}
	for i, r := range bad {
		if err := r.Validate(now); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestEvaluate(t *testing.T) {
	band := Band{MinC: 2, MaxC: 8, HysteresisC: 0.5}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(i int, temp float64) Reading {
		return Reading{ContainerID: "C", RecordedAt: t0.Add(time.Duration(i) * time.Minute), TemperatureC: temp}
	}

	steps := []struct {
		temp     float64
		want     TransitionType
		wantOpen bool
		wantKind BreachKind
		wantPeak float64
	}{
		{5.0, TransitionNone, false, "", 0},
		{8.0, TransitionNone, false, "", 0},               // on the limit is still OK
		{8.4, TransitionOpened, true, BreachTooWarm, 8.4}, // crosses max
		{9.1, TransitionNone, true, BreachTooWarm, 9.1},   // peak tracks
		{7.8, TransitionNone, true, BreachTooWarm, 9.1},   // back in band but within hysteresis
		{7.5, TransitionClosed, false, "", 0},             // recovered by margin
		{1.0, TransitionOpened, true, BreachTooCold, 1.0},
		{9.0, TransitionOpened, true, BreachTooWarm, 9.0}, // flips sides
	}

	var st BreachState
	for i, s := range steps {
		var tr TransitionType
		st, tr = Evaluate(band, st, at(i, s.temp))
		if tr != s.want || st.Open != s.wantOpen || st.Kind != s.wantKind || st.PeakTempC != s.wantPeak {
			t.Fatalf("step %d (%.1f°C): got transition=%q state=%+v, want transition=%q open=%v kind=%q peak=%v",
				i, s.temp, tr, st, s.want, s.wantOpen, s.wantKind, s.wantPeak)
		}
	}
}
