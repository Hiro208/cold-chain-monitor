// Package alert routes breach events to operators (SNS) and to partner
// webhooks. It runs inside the breach-alert Lambda.
package alert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
	"github.com/guangjiandong/cold-chain-monitor/internal/webhook"
)

// Notifier pages operators, e.g. through an SNS topic with email/SMS/
// PagerDuty subscribers.
type Notifier interface {
	Notify(ctx context.Context, ev stream.BreachEvent) error
}

// Subscriptions looks up partner webhooks for an event.
type Subscriptions interface {
	WebhooksForEvent(ctx context.Context, owner, eventType string) ([]store.WebhookSubscription, error)
}

// Opener decrypts webhook secrets.
type Opener interface {
	Open(ciphertext, aad []byte) ([]byte, error)
}

// Deliverer sends one webhook.
type Deliverer interface {
	Deliver(ctx context.Context, d webhook.Delivery) error
}

// Router fans a breach event out to every destination.
type Router struct {
	Notifier      Notifier
	Subscriptions Subscriptions
	Opener        Opener
	Deliverer     Deliverer
	Log           *slog.Logger
	// Concurrency bounds parallel webhook calls per invocation.
	Concurrency int
}

// EventID is stable across redeliveries so partners can dedupe.
func EventID(ev stream.BreachEvent) string {
	return strconv.FormatInt(ev.Breach.ID, 10) + ":" + string(ev.Type)
}

// Route handles a batch of encoded events. It returns an error only for
// failures worth retrying the whole batch for (operator notification or a
// transient webhook failure); permanently failing partner endpoints are
// logged and skipped so one broken partner cannot block everyone else.
func (r *Router) Route(ctx context.Context, values [][]byte) error {
	var events []stream.BreachEvent
	for _, v := range values {
		ev, err := stream.DecodeBreachEvent(v)
		if err != nil {
			r.Log.Error("dropping undecodable breach event", "err", err)
			continue
		}
		events = append(events, ev)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(r.Concurrency, 1))
	var (
		mu        sync.Mutex
		transient []error
	)
	for _, ev := range events {
		if err := r.Notifier.Notify(ctx, ev); err != nil {
			return fmt.Errorf("notify operators for breach %d: %w", ev.Breach.ID, err)
		}
		subs, err := r.Subscriptions.WebhooksForEvent(ctx, ev.Owner, string(ev.Type))
		if err != nil {
			return fmt.Errorf("load webhooks: %w", err)
		}
		if len(subs) == 0 {
			continue
		}
		body, err := stream.EncodeBreachEvent(ev)
		if err != nil {
			return err
		}
		for _, sub := range subs {
			g.Go(func() error {
				secret, err := r.Opener.Open(sub.SecretEnc, []byte(sub.ID))
				if err != nil {
					r.Log.Error("cannot decrypt webhook secret", "subscription", sub.ID, "err", err)
					return nil
				}
				err = r.Deliverer.Deliver(gctx, webhook.Delivery{URL: sub.URL, Secret: string(secret), EventID: EventID(ev), Body: body})
				var perm *webhook.PermanentError
				switch {
				case err == nil:
					r.Log.Info("webhook delivered", "subscription", sub.ID, "event", EventID(ev))
				case errors.As(err, &perm):
					r.Log.Warn("webhook rejected permanently", "subscription", sub.ID, "event", EventID(ev), "err", err)
				default:
					mu.Lock()
					transient = append(transient, fmt.Errorf("subscription %s: %w", sub.ID, err))
					mu.Unlock()
				}
				return nil
			})
		}
	}
	_ = g.Wait()
	return errors.Join(transient...)
}
