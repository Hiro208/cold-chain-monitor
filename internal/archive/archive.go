// Package archive copies the raw readings topic to S3 as gzipped NDJSON so
// that history outlives Kafka retention and can be queried with Athena.
//
// Objects are laid out Hive-style:
//
//	s3://<bucket>/<prefix>/dt=2026-06-01/hour=08/p=03/000000001200-000000001899.ndjson.gz
//
// The key embeds the Kafka partition and offset range, so re-uploading the
// same range after a crash overwrites the same object. If a crash changes
// the batch boundaries, some rows can appear in two objects; downstream
// queries dedupe on (container_id, recorded_at).
package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"path"
	"time"
)

// Uploader stores one object.
type Uploader interface {
	Put(ctx context.Context, key string, body []byte) error
}

// Record is a raw message from the readings topic.
type Record struct {
	Partition int32
	Offset    int64
	Timestamp time.Time
	Value     []byte
}

// Object is a finished archive object ready to upload.
type Object struct {
	Key       string
	Body      []byte
	Partition int32
	LastOff   int64
	Rows      int
}

type buffer struct {
	firstOff, lastOff int64
	firstTS           time.Time
	rows              int
	buf               bytes.Buffer
	gz                *gzip.Writer
}

// Batcher accumulates records per partition and cuts objects. It is not
// safe for concurrent use.
type Batcher struct {
	prefix  string
	maxRows int
	parts   map[int32]*buffer
}

// NewBatcher returns a Batcher that cuts an object once a partition buffer
// reaches maxRows.
func NewBatcher(prefix string, maxRows int) *Batcher {
	return &Batcher{prefix: prefix, maxRows: maxRows, parts: map[int32]*buffer{}}
}

// Add appends a record and returns any objects that became full.
func (b *Batcher) Add(r Record) ([]Object, error) {
	p := b.parts[r.Partition]
	if p == nil {
		p = &buffer{firstOff: r.Offset, firstTS: r.Timestamp.UTC()}
		p.gz, _ = gzip.NewWriterLevel(&p.buf, gzip.BestSpeed)
		b.parts[r.Partition] = p
	}
	if _, err := p.gz.Write(bytes.TrimRight(r.Value, "\n")); err != nil {
		return nil, err
	}
	if _, err := p.gz.Write([]byte{'\n'}); err != nil {
		return nil, err
	}
	p.lastOff = r.Offset
	p.rows++
	if p.rows >= b.maxRows {
		obj, err := b.cut(r.Partition)
		if err != nil {
			return nil, err
		}
		return []Object{obj}, nil
	}
	return nil, nil
}

// Flush cuts objects for every non-empty partition buffer.
func (b *Batcher) Flush() ([]Object, error) {
	var out []Object
	for part := range b.parts {
		obj, err := b.cut(part)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	return out, nil
}

// Drop discards buffered data for partitions that were revoked; the new
// owner will re-read them from the last committed offset.
func (b *Batcher) Drop(partitions []int32) {
	for _, p := range partitions {
		delete(b.parts, p)
	}
}

func (b *Batcher) cut(part int32) (Object, error) {
	p := b.parts[part]
	delete(b.parts, part)
	if err := p.gz.Close(); err != nil {
		return Object{}, err
	}
	key := path.Join(b.prefix,
		"dt="+p.firstTS.Format("2006-01-02"),
		"hour="+p.firstTS.Format("15"),
		fmt.Sprintf("p=%02d", part),
		fmt.Sprintf("%012d-%012d.ndjson.gz", p.firstOff, p.lastOff))
	return Object{Key: key, Body: p.buf.Bytes(), Partition: part, LastOff: p.lastOff, Rows: p.rows}, nil
}
