package domain

import (
	"math"
	"time"
)

// Reading is a single telemetry sample emitted by a container's controller.
type Reading struct {
	ContainerID  string    `json:"container_id"`
	RecordedAt   time.Time `json:"recorded_at"`
	TemperatureC float64   `json:"temperature_c"`
	HumidityPct  float64   `json:"humidity_pct"`
	DoorOpen     bool      `json:"door_open"`
}

// maxClockSkew bounds how far in the future a device timestamp may be before
// the reading is rejected. Reefer controllers drift, so allow some slack.
const maxClockSkew = 5 * time.Minute

// Validate rejects readings that are structurally broken. It intentionally
// does not reject out-of-band temperatures — those are breaches, not errors.
func (r *Reading) Validate(now time.Time) error {
	if r.ContainerID == "" {
		return &ValidationError{Field: "container_id", Reason: "required"}
	}
	if r.RecordedAt.IsZero() {
		return &ValidationError{Field: "recorded_at", Reason: "required"}
	}
	if r.RecordedAt.After(now.Add(maxClockSkew)) {
		return &ValidationError{Field: "recorded_at", Reason: "too far in the future"}
	}
	if math.IsNaN(r.TemperatureC) || math.IsInf(r.TemperatureC, 0) {
		return &ValidationError{Field: "temperature_c", Reason: "not a finite number"}
	}
	if r.HumidityPct < 0 || r.HumidityPct > 100 {
		return &ValidationError{Field: "humidity_pct", Reason: "must be within [0, 100]"}
	}
	return nil
}
