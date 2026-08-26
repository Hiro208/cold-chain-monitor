// Package config loads service configuration from environment variables, the
// 12-factor way, so that the same image runs locally, in CI and on EKS.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Postgres holds database settings.
type Postgres struct {
	DSN      string
	MaxConns int32
}

// Redis holds cache settings.
type Redis struct {
	Addr     string
	Password string
	DB       int
	// StateTTL bounds how long a silent container keeps its "latest" entry.
	StateTTL time.Duration
}

// Kafka holds broker and topic settings.
type Kafka struct {
	Brokers       []string
	ReadingsTopic string
	BreachesTopic string
	DLQTopic      string
	ConsumerGroup string
	// Partitions is used only when the service creates missing topics
	// (local/dev); in AWS the topics are provisioned by Terraform.
	Partitions int32
}

// str reads an environment variable, falling back to def when unset or empty.
func str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// loader accumulates parse errors so that a misconfigured pod reports every
// bad variable at once instead of one per restart.
type loader struct{ errs []string }

func (l *loader) int(key string, def int) int {
	v := str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: %v", key, err))
		return def
	}
	return n
}

func (l *loader) float(key string, def float64) float64 {
	v := str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: %v", key, err))
		return def
	}
	return f
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: %v", key, err))
		return def
	}
	return d
}

func (l *loader) required(key string) string {
	v := str(key, "")
	if v == "" {
		l.errs = append(l.errs, fmt.Sprintf("%s: required", key))
	}
	return v
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("config: %s", strings.Join(l.errs, "; "))
}

func list(key, def string) []string {
	var out []string
	for _, p := range strings.Split(str(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadPostgres(l *loader) Postgres {
	return Postgres{
		DSN:      l.required("DATABASE_URL"),
		MaxConns: int32(l.int("DATABASE_MAX_CONNS", 10)),
	}
}

func loadRedis(l *loader) Redis {
	return Redis{
		Addr:     str("REDIS_ADDR", "localhost:6379"),
		Password: str("REDIS_PASSWORD", ""),
		DB:       l.int("REDIS_DB", 0),
		StateTTL: l.duration("REDIS_STATE_TTL", 24*time.Hour),
	}
}

func loadKafka(l *loader, defaultGroup string) Kafka {
	return Kafka{
		Brokers:       list("KAFKA_BROKERS", "localhost:9092"),
		ReadingsTopic: str("KAFKA_READINGS_TOPIC", "reefer.readings.v1"),
		BreachesTopic: str("KAFKA_BREACHES_TOPIC", "reefer.breaches.v1"),
		DLQTopic:      str("KAFKA_DLQ_TOPIC", "reefer.readings.dlq.v1"),
		ConsumerGroup: str("KAFKA_CONSUMER_GROUP", defaultGroup),
		Partitions:    int32(l.int("KAFKA_PARTITIONS", 12)),
	}
}

// API is the configuration of the REST API service.
type API struct {
	ListenAddr      string
	MetricsAddr     string
	ShutdownTimeout time.Duration
	Postgres        Postgres
	Redis           Redis
	// WebhookSecretKey encrypts partner webhook signing secrets at rest.
	WebhookSecretKey string
	// AllowInsecureWebhooks permits http:// webhook URLs (local dev only).
	AllowInsecureWebhooks bool
}

// LoadAPI loads the API configuration.
func LoadAPI() (API, error) {
	l := &loader{}
	c := API{
		ListenAddr:            str("HTTP_ADDR", ":8080"),
		MetricsAddr:           str("METRICS_ADDR", ":9090"),
		ShutdownTimeout:       l.duration("SHUTDOWN_TIMEOUT", 15*time.Second),
		Postgres:              loadPostgres(l),
		Redis:                 loadRedis(l),
		WebhookSecretKey:      str("WEBHOOK_SECRET_KEY", ""),
		AllowInsecureWebhooks: str("WEBHOOK_ALLOW_INSECURE", "") == "true",
	}
	return c, l.err()
}

// Ingest is the configuration of the Kafka ingest worker.
type Ingest struct {
	MetricsAddr string
	Postgres    Postgres
	Redis       Redis
	Kafka       Kafka
	// BatchSize / BatchWait bound how long readings sit in memory before
	// being flushed to Postgres. Together they cap detection latency.
	BatchSize   int
	BatchWait   time.Duration
	HysteresisC float64
}

// LoadIngest loads the ingest worker configuration.
func LoadIngest() (Ingest, error) {
	l := &loader{}
	c := Ingest{
		MetricsAddr: str("METRICS_ADDR", ":9090"),
		Postgres:    loadPostgres(l),
		Redis:       loadRedis(l),
		Kafka:       loadKafka(l, "reefer-ingest"),
		BatchSize:   l.int("INGEST_BATCH_SIZE", 500),
		BatchWait:   l.duration("INGEST_BATCH_WAIT", 250*time.Millisecond),
		HysteresisC: l.float("BREACH_HYSTERESIS_C", 0.5),
	}
	if c.BatchSize <= 0 {
		l.errs = append(l.errs, "INGEST_BATCH_SIZE: must be positive")
	}
	return c, l.err()
}

// Simulator is the configuration of the load generator.
type Simulator struct {
	Kafka      Kafka
	APIBaseURL string
	Containers int
	Interval   time.Duration
	// BreachRate is the probability that a container starts drifting out of
	// band on a given tick.
	BreachRate float64
	Register   bool
}

// LoadSimulator loads the simulator configuration.
func LoadSimulator() (Simulator, error) {
	l := &loader{}
	c := Simulator{
		Kafka:      loadKafka(l, ""),
		APIBaseURL: str("API_BASE_URL", "http://localhost:8080"),
		Containers: l.int("SIM_CONTAINERS", 2000),
		Interval:   l.duration("SIM_INTERVAL", 5*time.Second),
		BreachRate: l.float("SIM_BREACH_RATE", 0.002),
		Register:   str("SIM_REGISTER", "true") == "true",
	}
	return c, l.err()
}

// Archiver is the configuration of the S3 raw-reading archiver.
type Archiver struct {
	MetricsAddr   string
	Kafka         Kafka
	Bucket        string
	Prefix        string
	FlushEvery    time.Duration
	MaxObjectRows int
}

// LoadArchiver loads the archiver configuration.
func LoadArchiver() (Archiver, error) {
	l := &loader{}
	c := Archiver{
		MetricsAddr:   str("METRICS_ADDR", ":9090"),
		Kafka:         loadKafka(l, "reefer-archiver"),
		Bucket:        l.required("ARCHIVE_BUCKET"),
		Prefix:        str("ARCHIVE_PREFIX", "raw/readings"),
		FlushEvery:    l.duration("ARCHIVE_FLUSH_EVERY", time.Minute),
		MaxObjectRows: l.int("ARCHIVE_MAX_ROWS", 50_000),
	}
	return c, l.err()
}
