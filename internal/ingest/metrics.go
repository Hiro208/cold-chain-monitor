package ingest

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the Prometheus instruments of the ingest pipeline.
type Metrics struct {
	ReadingsProcessed prometheus.Counter
	ReadingsRejected  prometheus.Counter
	ReadingsSkipped   prometheus.Counter
	BreachTransitions *prometheus.CounterVec
	// DetectionLatency is the time from a reading being recorded on the
	// device to its breach being persisted and published — the number the
	// "flag breaches within 2 s" objective is measured against.
	DetectionLatency prometheus.Histogram
	// IngestLag is device-to-processed latency for every reading.
	IngestLag     prometheus.Histogram
	BatchDuration prometheus.Histogram
	BatchSize     prometheus.Histogram
}

var latencyBuckets = []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30}

// NewMetrics creates and registers the ingest metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ReadingsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "readings_processed_total",
			Help: "Readings evaluated and persisted.",
		}),
		ReadingsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "readings_rejected_total",
			Help: "Readings sent to the dead-letter topic.",
		}),
		ReadingsSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "readings_skipped_total",
			Help: "Replayed or out-of-order readings ignored by the detector.",
		}),
		BreachTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "breach_transitions_total",
			Help: "Breach open/close transitions.",
		}, []string{"type", "kind"}),
		DetectionLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "breach_detection_latency_seconds",
			Help: "Time from the breaching reading's recorded_at to the breach event being published.", Buckets: latencyBuckets,
		}),
		IngestLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "lag_seconds",
			Help: "Time from recorded_at to the reading being processed.", Buckets: latencyBuckets,
		}),
		BatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "batch_duration_seconds",
			Help: "Wall time to process one batch.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "coldchain", Subsystem: "ingest", Name: "batch_size",
			Help: "Readings per processed batch.", Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
	}
	reg.MustRegister(m.ReadingsProcessed, m.ReadingsRejected, m.ReadingsSkipped,
		m.BreachTransitions, m.DetectionLatency, m.IngestLag, m.BatchDuration, m.BatchSize)
	return m
}

func nopMetrics() *Metrics { return NewMetrics(prometheus.NewRegistry()) }

// WithMetrics attaches registered metrics to the processor.
func (p *Processor) WithMetrics(m *Metrics) *Processor {
	p.metrics = m
	return p
}
