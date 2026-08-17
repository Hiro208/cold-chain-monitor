package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducer returns a client tuned for throughput with safety: idempotent
// writes (the franz-go default), acks from all in-sync replicas, and a short
// linger so readings are batched without hurting breach-detection latency.
func NewProducer(brokers []string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // murmur2, Java-compatible
	}, extra...)
	return kgo.NewClient(opts...)
}

// TopicSpec describes a topic to create if missing.
type TopicSpec struct {
	Name       string
	Partitions int32
	// Replication of -1 uses the broker default.
	Replication int16
	Config      map[string]*string
}

// EnsureTopics creates topics that do not exist yet. Existing topics are left
// untouched — partition changes are an explicit operational decision because
// they reshuffle the key-to-partition mapping.
func EnsureTopics(ctx context.Context, cl *kgo.Client, specs ...TopicSpec) error {
	adm := kadm.NewClient(cl)
	for _, s := range specs {
		resp, err := adm.CreateTopic(ctx, s.Partitions, s.Replication, s.Config, s.Name)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
	}
	return nil
}

func ptr(s string) *string { return &s }

// DefaultTopics returns the topic layout used by docker-compose and tests.
// Readings keep 7 days in Kafka (S3 holds the long-term archive).
func DefaultTopics(readings, breaches string, partitions int32) []TopicSpec {
	return []TopicSpec{
		{Name: readings, Partitions: partitions, Replication: -1, Config: map[string]*string{
			"retention.ms":     ptr("604800000"),
			"compression.type": ptr("producer"),
		}},
		{Name: breaches, Partitions: partitions, Replication: -1, Config: map[string]*string{
			"retention.ms": ptr("2592000000"),
		}},
	}
}
