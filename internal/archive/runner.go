package archive

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// S3Uploader writes objects to a bucket with server-side encryption.
type S3Uploader struct {
	Client *s3.Client
	Bucket string
}

// Put implements Uploader.
func (u S3Uploader) Put(ctx context.Context, key string, body []byte) error {
	_, err := u.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(u.Bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(body),
		ContentType:          aws.String("application/x-ndjson"),
		ContentEncoding:      aws.String("gzip"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	return err
}

// Runner consumes the readings topic and uploads batched objects. Offsets
// for a partition are committed only after its object is in S3.
type Runner struct {
	cl         *kgo.Client
	batcher    *Batcher
	up         Uploader
	flushEvery time.Duration
	log        *slog.Logger
	topic      string

	mu      sync.Mutex
	revoked []int32
}

// NewRunner creates the consumer. Uses its own consumer group so archiving
// progresses independently of (and never slows down) breach detection.
func NewRunner(conn []kgo.Opt, group, topic string, b *Batcher, up Uploader, flushEvery time.Duration, log *slog.Logger) (*Runner, error) {
	r := &Runner{batcher: b, up: up, flushEvery: flushEvery, log: log, topic: topic}
	opts := append(append([]kgo.Opt{}, conn...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(time.Second),
		kgo.OnPartitionsRevoked(r.onRevoked),
		kgo.OnPartitionsLost(r.onRevoked),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	r.cl = cl
	return r, nil
}

func (r *Runner) onRevoked(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	r.mu.Lock()
	r.revoked = append(r.revoked, lost[r.topic]...)
	r.mu.Unlock()
}

// Run consumes until ctx is cancelled, then flushes what it has.
func (r *Runner) Run(ctx context.Context) error {
	// BlockRebalanceOnPoll makes Close wait for a rebalance that only
	// AllowRebalance can release, so allow it first or shutdown hangs.
	defer func() {
		r.cl.AllowRebalance()
		r.cl.Close()
	}()
	ticker := time.NewTicker(r.flushEvery)
	defer ticker.Stop()

	for {
		pollCtx, cancel := context.WithTimeout(ctx, r.flushEvery)
		fetches := r.cl.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				r.log.Error("fetch error", "topic", t, "partition", p, "err", err)
			}
		})

		r.mu.Lock()
		if len(r.revoked) > 0 {
			r.batcher.Drop(r.revoked)
			r.revoked = nil
		}
		r.mu.Unlock()

		var ready []Object
		var addErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if addErr != nil {
				return
			}
			objs, err := r.batcher.Add(Record{Partition: rec.Partition, Offset: rec.Offset, Timestamp: rec.Timestamp, Value: rec.Value})
			addErr = err
			ready = append(ready, objs...)
		})
		if addErr != nil {
			return addErr
		}

		select {
		case <-ticker.C:
			objs, err := r.batcher.Flush()
			if err != nil {
				return err
			}
			ready = append(ready, objs...)
		default:
		}
		if ctx.Err() != nil {
			objs, err := r.batcher.Flush()
			if err != nil {
				return err
			}
			ready = append(ready, objs...)
		}

		if err := r.upload(context.WithoutCancel(ctx), ready); err != nil {
			return err
		}
		r.cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (r *Runner) upload(ctx context.Context, objs []Object) error {
	if len(objs) == 0 {
		return nil
	}
	offsets := map[string]map[int32]kgo.EpochOffset{r.topic: {}}
	for _, o := range objs {
		if err := r.up.Put(ctx, o.Key, o.Body); err != nil {
			return err
		}
		r.log.Info("archived object", "key", o.Key, "rows", o.Rows, "bytes", len(o.Body))
		offsets[r.topic][o.Partition] = kgo.EpochOffset{Epoch: -1, Offset: o.LastOff + 1}
	}
	r.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		// A failed commit only means the range is re-archived to the same key.
		if err != nil {
			r.log.Warn("commit offsets failed", "err", err)
		}
	})
	return nil
}
