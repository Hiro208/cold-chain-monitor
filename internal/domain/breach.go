package domain

import "time"

// BreachKind says which side of the band a breach is on.
type BreachKind string

const (
	BreachTooWarm BreachKind = "TOO_WARM"
	BreachTooCold BreachKind = "TOO_COLD"
)

// Breach is a contiguous period during which a container's temperature was
// outside its allowed band. EndedAt is nil while the breach is still open.
type Breach struct {
	ID          int64      `json:"id"`
	ContainerID string     `json:"container_id"`
	Kind        BreachKind `json:"kind"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	PeakTempC   float64    `json:"peak_temp_c"`
	LimitC      float64    `json:"limit_c"`
}

// Band is the allowed temperature range of a container plus a hysteresis
// margin that a temperature must recover by before a breach is closed. The
// margin prevents an alert storm when a reading hovers right on a limit.
type Band struct {
	MinC        float64 `json:"min_c"`
	MaxC        float64 `json:"max_c"`
	HysteresisC float64 `json:"hysteresis_c"`
}

// BandOf returns the band of a registered container.
func BandOf(c Container, hysteresisC float64) Band {
	return Band{MinC: c.MinTempC, MaxC: c.MaxTempC, HysteresisC: hysteresisC}
}

// Classify returns the breach kind for a temperature, or "" if it is in band.
func (b Band) Classify(tempC float64) BreachKind {
	switch {
	case tempC > b.MaxC:
		return BreachTooWarm
	case tempC < b.MinC:
		return BreachTooCold
	default:
		return ""
	}
}

// Recovered reports whether a temperature is back inside the band by at least
// the hysteresis margin for the given breach kind.
func (b Band) Recovered(kind BreachKind, tempC float64) bool {
	switch kind {
	case BreachTooWarm:
		return tempC <= b.MaxC-b.HysteresisC
	case BreachTooCold:
		return tempC >= b.MinC+b.HysteresisC
	default:
		return true
	}
}

// Limit returns the band edge that a breach of the given kind crossed.
func (b Band) Limit(kind BreachKind) float64 {
	if kind == BreachTooCold {
		return b.MinC
	}
	return b.MaxC
}

// TransitionType describes what a new reading did to the breach state.
type TransitionType string

const (
	TransitionNone   TransitionType = ""
	TransitionOpened TransitionType = "OPENED"
	TransitionClosed TransitionType = "CLOSED"
)

// BreachState is the per-container detector state. It is small and is kept
// in Redis alongside the latest reading.
type BreachState struct {
	Open      bool       `json:"open"`
	Kind      BreachKind `json:"kind,omitempty"`
	StartedAt time.Time  `json:"started_at,omitempty"`
	PeakTempC float64    `json:"peak_temp_c,omitempty"`
}

// Evaluate applies one reading to the current state and returns the new state
// and the transition it caused. It is a pure function so that the detector is
// trivially testable and safe to replay after a consumer restart.
//
// A breach that flips directly from too-warm to too-cold (sensor glitch or a
// mis-set setpoint) is reported as TransitionOpened with the new kind; the
// store closes the previous open breach for that container in the same
// transaction.
func Evaluate(band Band, prev BreachState, r Reading) (BreachState, TransitionType) {
	kind := band.Classify(r.TemperatureC)

	if !prev.Open {
		if kind == "" {
			return prev, TransitionNone
		}
		return BreachState{Open: true, Kind: kind, StartedAt: r.RecordedAt, PeakTempC: r.TemperatureC}, TransitionOpened
	}

	// Breach currently open.
	if kind != "" && kind != prev.Kind {
		return BreachState{Open: true, Kind: kind, StartedAt: r.RecordedAt, PeakTempC: r.TemperatureC}, TransitionOpened
	}
	if band.Recovered(prev.Kind, r.TemperatureC) {
		return BreachState{}, TransitionClosed
	}
	next := prev
	if (prev.Kind == BreachTooWarm && r.TemperatureC > prev.PeakTempC) ||
		(prev.Kind == BreachTooCold && r.TemperatureC < prev.PeakTempC) {
		next.PeakTempC = r.TemperatureC
	}
	return next, TransitionNone
}
