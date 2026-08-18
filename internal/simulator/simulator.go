// Package simulator generates realistic-looking reefer telemetry for load
// testing and demos.
//
// Each unit's temperature follows a mean-reverting random walk (a discrete
// Ornstein–Uhlenbeck process) around its setpoint. With a small probability
// per tick a unit suffers a "compressor fault": its temperature drifts away
// from the setpoint for a while and then recovers, which produces realistic
// breach open/close sequences.
package simulator

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// Profile is a cargo type with its temperature band.
type Profile struct {
	Commodity string
	SetpointC float64
	MinC      float64
	MaxC      float64
}

// Profiles cover the common reefer cargo classes.
var Profiles = []Profile{
	{Commodity: "frozen-seafood", SetpointC: -20, MinC: -25, MaxC: -16},
	{Commodity: "frozen-meat", SetpointC: -18, MinC: -23, MaxC: -15},
	{Commodity: "fresh-produce", SetpointC: 4, MinC: 1, MaxC: 8},
	{Commodity: "vaccines", SetpointC: 5, MinC: 2, MaxC: 8},
	{Commodity: "bananas", SetpointC: 13.5, MinC: 12.5, MaxC: 15},
}

// OwnerCode is the ISO 6346 owner prefix used for simulated containers.
const OwnerCode = "SIM"

// Unit is the mutable state of one simulated container.
type Unit struct {
	Container domain.Container
	tempC     float64
	humidity  float64
	// faultTicks > 0 means a compressor fault is in progress.
	faultTicks int
	faultDir   float64
	doorTicks  int
}

// Fleet is a set of simulated units. It is not safe for concurrent use.
type Fleet struct {
	Units      []*Unit
	rng        *rand.Rand
	breachRate float64
}

// NewFleet builds n units with deterministic IDs and profiles derived from
// seed, so repeated runs register the same containers.
func NewFleet(n int, breachRate float64, seed uint64) *Fleet {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	f := &Fleet{rng: rng, breachRate: breachRate}
	for i := 0; i < n; i++ {
		p := Profiles[i%len(Profiles)]
		f.Units = append(f.Units, &Unit{
			Container: domain.Container{
				ID:        domain.MakeContainerID(OwnerCode, i),
				Owner:     "simulator",
				Commodity: p.Commodity,
				SetpointC: p.SetpointC,
				MinTempC:  p.MinC,
				MaxTempC:  p.MaxC,
			},
			tempC:    p.SetpointC + rng.NormFloat64()*0.3,
			humidity: 60 + rng.Float64()*25,
		})
	}
	return f
}

// Step advances unit i by one tick and returns the reading it emits.
func (f *Fleet) Step(i int, at time.Time) domain.Reading {
	u := f.Units[i]
	c := u.Container

	// Start a fault?
	if u.faultTicks == 0 && f.rng.Float64() < f.breachRate {
		u.faultTicks = 20 + f.rng.IntN(40)
		u.faultDir = 1
		if f.rng.Float64() < 0.2 { // thermostat over-cooling is rarer than warming
			u.faultDir = -1
		}
	}
	// Door openings let warm air in briefly.
	if u.doorTicks == 0 && f.rng.Float64() < 0.001 {
		u.doorTicks = 2 + f.rng.IntN(4)
	}

	const (
		theta = 0.15 // mean-reversion strength per tick
		sigma = 0.08 // noise °C per tick
	)
	target := c.SetpointC
	if u.faultTicks > 0 {
		// Drift towards a point well outside the band; the half-way mark of
		// the fault flips into recovery.
		span := c.MaxTempC - c.MinTempC
		target = c.SetpointC + u.faultDir*span*1.2
		u.faultTicks--
		if u.faultTicks == 0 {
			u.faultDir = 0
		}
	}
	if u.doorTicks > 0 {
		target += 3
		u.doorTicks--
	}
	u.tempC += theta*(target-u.tempC) + sigma*f.rng.NormFloat64()
	u.humidity = clamp(u.humidity+f.rng.NormFloat64()*0.5, 30, 98)

	return domain.Reading{
		ContainerID:  c.ID,
		RecordedAt:   at.UTC(),
		TemperatureC: math.Round(u.tempC*100) / 100,
		HumidityPct:  math.Round(u.humidity*10) / 10,
		DoorOpen:     u.doorTicks > 0,
	}
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }
