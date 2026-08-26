package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"testing"
	"time"
)

func gunzipLines(t *testing.T, body []byte) []string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	sc := bufio.NewScanner(zr)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines
}

func TestBatcherCutsBySizeAndFlush(t *testing.T) {
	ts := time.Date(2026, 6, 1, 8, 59, 0, 0, time.UTC)
	b := NewBatcher("raw/readings", 3)

	var objs []Object
	for i := 0; i < 4; i++ {
		got, err := b.Add(Record{Partition: 2, Offset: int64(100 + i), Timestamp: ts, Value: []byte(fmt.Sprintf(`{"n":%d}`, i))})
		if err != nil {
			t.Fatal(err)
		}
		objs = append(objs, got...)
	}
	if _, err := b.Add(Record{Partition: 5, Offset: 7, Timestamp: ts, Value: []byte(`{"n":"p5"}` + "\n")}); err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected one size-triggered object, got %d", len(objs))
	}
	want := "raw/readings/dt=2026-06-01/hour=08/p=02/000000000100-000000000102.ndjson.gz"
	if objs[0].Key != want {
		t.Fatalf("key = %s\nwant  %s", objs[0].Key, want)
	}
	if lines := gunzipLines(t, objs[0].Body); strings.Join(lines, ",") != `{"n":0},{"n":1},{"n":2}` {
		t.Fatalf("lines = %v", lines)
	}

	rest, err := b.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 {
		t.Fatalf("flush returned %d objects, want 2", len(rest))
	}
	for _, o := range rest {
		if o.Rows != 1 {
			t.Fatalf("object %s has %d rows", o.Key, o.Rows)
		}
		if lines := gunzipLines(t, o.Body); len(lines) != 1 {
			t.Fatalf("trailing newline handling broken: %q", lines)
		}
	}
	if again, _ := b.Flush(); len(again) != 0 {
		t.Fatal("buffers not reset after flush")
	}
}

func TestBatcherDrop(t *testing.T) {
	b := NewBatcher("x", 100)
	_, _ = b.Add(Record{Partition: 1, Offset: 1, Value: []byte("{}")})
	_, _ = b.Add(Record{Partition: 2, Offset: 1, Value: []byte("{}")})
	b.Drop([]int32{1})
	objs, _ := b.Flush()
	if len(objs) != 1 || objs[0].Partition != 2 {
		t.Fatalf("objs = %+v", objs)
	}
}
