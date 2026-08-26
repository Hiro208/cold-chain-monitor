package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrForbidden is returned when a partner acts on an owner it has no grant
// for.
var ErrForbidden = errors.New("forbidden")

// WebhookSubscription is a partner endpoint subscribed to breach events of
// one container owner.
type WebhookSubscription struct {
	ID        string    `json:"id"`
	PartnerID string    `json:"-"`
	Owner     string    `json:"owner"`
	URL       string    `json:"url"`
	Events    []string  `json:"events"`
	SecretEnc []byte    `json:"-"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

const webhookColumns = "id::text, partner_id, owner, url, events, secret_enc, active, created_at"

func scanWebhook(row pgx.Row) (WebhookSubscription, error) {
	var w WebhookSubscription
	err := row.Scan(&w.ID, &w.PartnerID, &w.Owner, &w.URL, &w.Events, &w.SecretEnc, &w.Active, &w.CreatedAt)
	return w, err
}

// CreateWebhook stores a subscription after checking the partner's grant
// for the owner, atomically.
func (s *Store) CreateWebhook(ctx context.Context, w WebhookSubscription) (WebhookSubscription, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO webhook_subscriptions (id, partner_id, owner, url, events, secret_enc)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE EXISTS (SELECT 1 FROM partner_grants WHERE partner_id = $2 AND owner = $3)
		RETURNING `+webhookColumns,
		w.ID, w.PartnerID, w.Owner, w.URL, w.Events, w.SecretEnc)
	out, err := scanWebhook(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return WebhookSubscription{}, ErrForbidden
	}
	if err != nil {
		return WebhookSubscription{}, fmt.Errorf("insert webhook: %w", err)
	}
	return out, nil
}

// ListWebhooks returns a partner's subscriptions.
func (s *Store) ListWebhooks(ctx context.Context, partnerID string) ([]WebhookSubscription, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+webhookColumns+" FROM webhook_subscriptions WHERE partner_id = $1 ORDER BY created_at", partnerID)
	if err != nil {
		return nil, fmt.Errorf("list webhooks: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (WebhookSubscription, error) { return scanWebhook(r) })
}

// DeleteWebhook removes a partner's subscription. Deleting another
// partner's subscription reports ErrNotFound, not ErrForbidden, so IDs
// cannot be probed.
func (s *Store) DeleteWebhook(ctx context.Context, partnerID, id string) error {
	tag, err := s.pool.Exec(ctx, "DELETE FROM webhook_subscriptions WHERE id::text = $1 AND partner_id = $2", id, partnerID)
	if err != nil {
		return fmt.Errorf("delete webhook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// WebhooksForEvent returns active subscriptions of an owner that want the
// given event type.
func (s *Store) WebhooksForEvent(ctx context.Context, owner, eventType string) ([]WebhookSubscription, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+webhookColumns+` FROM webhook_subscriptions
		WHERE owner = $1 AND active AND $2 = ANY(events)`, owner, eventType)
	if err != nil {
		return nil, fmt.Errorf("webhooks for event: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (WebhookSubscription, error) { return scanWebhook(r) })
}
