package alert

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
	"github.com/guangjiandong/cold-chain-monitor/internal/webhook"
)

type recNotifier struct{ n int }

func (r *recNotifier) Notify(context.Context, stream.BreachEvent) error { r.n++; return nil }

type subs map[string][]store.WebhookSubscription

func (s subs) WebhooksForEvent(_ context.Context, owner, _ string) ([]store.WebhookSubscription, error) {
	return s[owner], nil
}

type identity struct{}

func (identity) Open(c, _ []byte) ([]byte, error) { return c, nil }

type recDeliverer struct {
	mu   sync.Mutex
	got  []webhook.Delivery
	errs map[string]error
}

func (d *recDeliverer) Deliver(_ context.Context, del webhook.Delivery) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, del)
	return d.errs[del.URL]
}

func encode(t *testing.T, id int64, owner string) []byte {
	t.Helper()
	b, err := stream.EncodeBreachEvent(stream.BreachEvent{
		Type: stream.BreachOpened, Owner: owner,
		Breach: domain.Breach{ID: id, ContainerID: "CSQU3054383", Kind: domain.BreachTooWarm},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newRouter(s subs, d *recDeliverer) (*Router, *recNotifier) {
	n := &recNotifier{}
	return &Router{
		Notifier: n, Subscriptions: s, Opener: identity{}, Deliverer: d,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Concurrency: 4,
	}, n
}

func TestRouteFansOut(t *testing.T) {
	s := subs{"acme": {
		{ID: "a", URL: "https://a.example", SecretEnc: []byte("sa")},
		{ID: "b", URL: "https://b.example", SecretEnc: []byte("sb")},
	}}
	d := &recDeliverer{}
	r, n := newRouter(s, d)

	err := r.Route(context.Background(), [][]byte{encode(t, 7, "acme"), encode(t, 8, "nobody"), []byte("junk")})
	if err != nil {
		t.Fatal(err)
	}
	if n.n != 2 {
		t.Fatalf("operator notifications = %d, want 2", n.n)
	}
	if len(d.got) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(d.got))
	}
	for _, del := range d.got {
		if del.EventID != "7:breach.opened" || del.Secret != "s"+del.URL[8:9] {
			t.Fatalf("unexpected delivery %+v", del)
		}
	}
}

func TestRouteErrorsOnlyOnTransientFailures(t *testing.T) {
	s := subs{"acme": {
		{ID: "gone", URL: "https://gone.example"},
		{ID: "flaky", URL: "https://flaky.example"},
	}}
	d := &recDeliverer{errs: map[string]error{
		"https://gone.example":  &webhook.PermanentError{Err: errors.New("410")},
		"https://flaky.example": errors.New("503"),
	}}
	r, _ := newRouter(s, d)
	if err := r.Route(context.Background(), [][]byte{encode(t, 1, "acme")}); err == nil {
		t.Fatal("expected transient failure to fail the batch")
	}

	delete(d.errs, "https://flaky.example")
	if err := r.Route(context.Background(), [][]byte{encode(t, 1, "acme")}); err != nil {
		t.Fatalf("permanent failure should not fail the batch: %v", err)
	}
}
