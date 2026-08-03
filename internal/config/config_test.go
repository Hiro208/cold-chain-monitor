package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadIngestDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/coldchain")
	c, err := LoadIngest()
	if err != nil {
		t.Fatal(err)
	}
	if c.BatchSize != 500 || c.BatchWait != 250*time.Millisecond {
		t.Fatalf("unexpected batching defaults: %+v", c)
	}
	if got := c.Kafka.Brokers; len(got) != 1 || got[0] != "localhost:9092" {
		t.Fatalf("brokers = %v", got)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("INGEST_BATCH_WAIT", "soon")
	t.Setenv("REDIS_DB", "zero")
	_, err := LoadIngest()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"DATABASE_URL", "INGEST_BATCH_WAIT", "REDIS_DB"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}

func TestBrokerList(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " b1:9092, b2:9092 ,,")
	got := list("KAFKA_BROKERS", "")
	if len(got) != 2 || got[0] != "b1:9092" || got[1] != "b2:9092" {
		t.Fatalf("list = %v", got)
	}
}
