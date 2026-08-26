package api

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
	"github.com/guangjiandong/cold-chain-monitor/internal/webhook"
)

// PartnerHeader identifies the calling partner. API Gateway sets it from
// $context.identity.apiKeyId via request-parameter mapping, overwriting any
// value a client sends, and the in-cluster ingress only accepts traffic from
// API Gateway's VPC link for /v1/webhooks.
const PartnerHeader = "X-Partner-Id"

// WebhookStore persists partner subscriptions.
type WebhookStore interface {
	CreateWebhook(ctx context.Context, w store.WebhookSubscription) (store.WebhookSubscription, error)
	ListWebhooks(ctx context.Context, partnerID string) ([]store.WebhookSubscription, error)
	DeleteWebhook(ctx context.Context, partnerID, id string) error
}

// Sealer encrypts webhook secrets.
type Sealer interface {
	Seal(plaintext, aad []byte) ([]byte, error)
}

// WithWebhooks enables the partner webhook endpoints.
func WithWebhooks(ws WebhookStore, sealer Sealer, allowInsecureURLs bool) Option {
	return func(s *Server) {
		s.webhooks, s.sealer, s.allowInsecureWebhooks = ws, sealer, allowInsecureURLs
	}
}

var knownEvents = []string{string(stream.BreachOpened), string(stream.BreachClosed)}

type createWebhookRequest struct {
	Owner  string   `json:"owner"`
	URL    string   `json:"url"`
	Events []string `json:"events"`
}

type createWebhookResponse struct {
	store.WebhookSubscription
	// Secret is shown exactly once; partners use it to verify signatures.
	Secret string `json:"secret"`
}

func partnerID(r *http.Request) (string, error) {
	id := strings.TrimSpace(r.Header.Get(PartnerHeader))
	if id == "" {
		return "", &domain.ValidationError{Field: PartnerHeader, Reason: "missing partner identity"}
	}
	return id, nil
}

func (s *Server) createWebhook(w http.ResponseWriter, r *http.Request) {
	pid, err := partnerID(r)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, err.Error())
		return
	}
	var req createWebhookRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeErr(w, r, err)
		return
	}
	if req.Owner == "" {
		writeErr(w, r, &domain.ValidationError{Field: "owner", Reason: "required"})
		return
	}
	if err := webhook.ValidateURL(req.URL, s.allowInsecureWebhooks); err != nil {
		writeErr(w, r, &domain.ValidationError{Field: "url", Reason: err.Error()})
		return
	}
	if len(req.Events) == 0 {
		req.Events = knownEvents
	}
	for _, e := range req.Events {
		if !slices.Contains(knownEvents, e) {
			writeErr(w, r, &domain.ValidationError{Field: "events", Reason: fmt.Sprintf("unknown event %q", e)})
			return
		}
	}

	id := newUUID()
	secret, err := webhook.NewSecret()
	if err != nil {
		writeErr(w, r, err)
		return
	}
	enc, err := s.sealer.Seal([]byte(secret), []byte(id))
	if err != nil {
		writeErr(w, r, err)
		return
	}
	sub, err := s.webhooks.CreateWebhook(r.Context(), store.WebhookSubscription{
		ID: id, PartnerID: pid, Owner: req.Owner, URL: req.URL, Events: req.Events, SecretEnc: enc,
	})
	if err != nil {
		writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createWebhookResponse{WebhookSubscription: sub, Secret: secret})
}

func (s *Server) listWebhooks(w http.ResponseWriter, r *http.Request) {
	pid, err := partnerID(r)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, err.Error())
		return
	}
	subs, err := s.webhooks.ListWebhooks(r.Context(), pid)
	if err != nil {
		writeErr(w, r, err)
		return
	}
	if subs == nil {
		subs = []store.WebhookSubscription{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": subs})
}

func (s *Server) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	pid, err := partnerID(r)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, err.Error())
		return
	}
	if err := s.webhooks.DeleteWebhook(r.Context(), pid, r.PathValue("id")); err != nil {
		writeErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// newUUID returns a random RFC 4122 version 4 UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
