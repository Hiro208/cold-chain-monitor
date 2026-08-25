package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// httpMetrics records RED metrics (rate, errors, duration) per route.
type httpMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newHTTPMetrics(reg prometheus.Registerer) *httpMetrics {
	m := &httpMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "coldchain", Subsystem: "api", Name: "http_requests_total",
			Help: "HTTP requests by route, method and status code.",
		}, []string{"route", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "coldchain", Subsystem: "api", Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"route", "method"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// instrument must wrap the ServeMux directly: the mux records the matched
// pattern on the request it is given, and labelling by pattern (not raw
// path) keeps label cardinality bounded.
func (m *httpMetrics) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec, ok := w.(*statusRecorder)
		if !ok {
			rec = &statusRecorder{ResponseWriter: w}
			w = rec
		}
		start := time.Now()
		next.ServeHTTP(w, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		code := rec.status
		if code == 0 {
			code = http.StatusOK
		}
		m.requests.WithLabelValues(route, r.Method, strconv.Itoa(code)).Inc()
		m.duration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}
