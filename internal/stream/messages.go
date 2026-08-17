// Package stream defines the Kafka message contracts and client helpers.
//
// Topics:
//
//	reefer.readings.v1  key = container ID, value = ReadingMessage (JSON)
//	reefer.breaches.v1  key = container ID, value = BreachEvent (JSON)
//
// Keying by container ID pins every container to one partition, which gives
// per-container ordering and lets a single consumer own each container's
// breach state without coordination.
package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// SchemaVersion is bumped on breaking changes to the message layout.
// Consumers reject unknown versions instead of guessing.
const SchemaVersion = 1

// ReadingMessage is the wire format of a telemetry sample.
type ReadingMessage struct {
	V int `json:"v"`
	domain.Reading
}

// EncodeReading serialises a reading for the readings topic.
func EncodeReading(r domain.Reading) ([]byte, error) {
	return json.Marshal(ReadingMessage{V: SchemaVersion, Reading: r})
}

// DecodeReading parses and validates a readings-topic message.
func DecodeReading(b []byte) (domain.Reading, error) {
	var m ReadingMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return domain.Reading{}, fmt.Errorf("decode reading: %w", err)
	}
	if m.V != SchemaVersion {
		return domain.Reading{}, fmt.Errorf("decode reading: unsupported schema version %d", m.V)
	}
	m.Reading.RecordedAt = m.Reading.RecordedAt.UTC()
	return m.Reading, nil
}

// BreachEventType is the kind of breach transition.
type BreachEventType string

const (
	BreachOpened BreachEventType = "breach.opened"
	BreachClosed BreachEventType = "breach.closed"
)

// BreachEvent is published whenever a breach opens or closes. The alerting
// Lambda consumes it to notify operators and partner webhooks.
type BreachEvent struct {
	V          int             `json:"v"`
	Type       BreachEventType `json:"type"`
	Breach     domain.Breach   `json:"breach"`
	Owner      string          `json:"owner"`
	Commodity  string          `json:"commodity,omitempty"`
	DetectedAt time.Time       `json:"detected_at"`
	// LatencyMs is DetectedAt minus the triggering reading's RecordedAt.
	LatencyMs int64 `json:"latency_ms"`
}

// EncodeBreachEvent serialises a breach event.
func EncodeBreachEvent(e BreachEvent) ([]byte, error) {
	e.V = SchemaVersion
	return json.Marshal(e)
}

// DecodeBreachEvent parses a breaches-topic message.
func DecodeBreachEvent(b []byte) (BreachEvent, error) {
	var e BreachEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return BreachEvent{}, fmt.Errorf("decode breach event: %w", err)
	}
	if e.V != SchemaVersion {
		return BreachEvent{}, fmt.Errorf("decode breach event: unsupported schema version %d", e.V)
	}
	return e, nil
}
