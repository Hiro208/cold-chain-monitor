package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

type fakeWebhooks struct {
	mu     sync.Mutex
	grants map[string]bool // partner|owner
	subs   []store.WebhookSubscription
}

func (f *fakeWebhooks) CreateWebhook(_ context.Context, w store.WebhookSubscription) (store.WebhookSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.grants[w.PartnerID+"|"+w.Owner] {
		return store.WebhookSubscription{}, store.ErrForbidden
	}
	w.Active = true
	f.subs = append(f.subs, w)
	return w, nil
}

func (f *fakeWebhooks) ListWebhooks(_ context.Context, pid string) ([]store.WebhookSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.WebhookSubscription
	for _, s := range f.subs {
		if s.PartnerID == pid {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeWebhooks) DeleteWebhook(_ context.Context, pid, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.subs {
		if s.ID == id && s.PartnerID == pid {
			f.subs = append(f.subs[:i], f.subs[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

type plainSealer struct{}

func (plainSealer) Seal(p, aad []byte) ([]byte, error) {
	return append(append([]byte{}, aad...), p...), nil
}

func partnerReq(h http.Handler, method, path, partner, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if partner != "" {
		req.Header.Set(PartnerHeader, partner)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebhookLifecycle(t *testing.T) {
	fw := &fakeWebhooks{grants: map[string]bool{"key-acme|acme": true}}
	h := newTestServer(t, newFakeStore(), WithWebhooks(fw, plainSealer{}, false))
	body := `{"owner":"acme","url":"https://hooks.acme.example/cold","events":["breach.opened"]}`

	if rec := partnerReq(h, http.MethodPost, "/v1/webhooks", "", body); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no partner: %d", rec.Code)
	}
	if rec := partnerReq(h, http.MethodPost, "/v1/webhooks", "key-other", body); rec.Code != http.StatusForbidden {
		t.Fatalf("ungranted partner: %d", rec.Code)
	}

	rec := partnerReq(h, http.MethodPost, "/v1/webhooks", "key-acme", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created createWebhookResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.Secret, "whsec_") || len(created.ID) != 36 {
		t.Fatalf("unexpected create response %+v", created)
	}
	if strings.Contains(rec.Body.String(), "secret_enc") || strings.Contains(rec.Body.String(), "partner") {
		t.Fatalf("response leaks internal fields: %s", rec.Body)
	}

	rec = partnerReq(h, http.MethodGet, "/v1/webhooks", "key-acme", "")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "whsec_") || !strings.Contains(rec.Body.String(), created.ID) {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}

	if rec := partnerReq(h, http.MethodDelete, "/v1/webhooks/"+created.ID, "key-other", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-partner delete: %d", rec.Code)
	}
	if rec := partnerReq(h, http.MethodDelete, "/v1/webhooks/"+created.ID, "key-acme", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
}

func TestWebhookValidation(t *testing.T) {
	fw := &fakeWebhooks{grants: map[string]bool{"k|acme": true}}
	h := newTestServer(t, newFakeStore(), WithWebhooks(fw, plainSealer{}, false))
	for name, body := range map[string]string{
		"http url":      `{"owner":"acme","url":"http://hooks.acme.example"}`,
		"metadata ip":   `{"owner":"acme","url":"https://169.254.169.254/x"}`,
		"unknown event": `{"owner":"acme","url":"https://hooks.acme.example","events":["nope"]}`,
		"no owner":      `{"url":"https://hooks.acme.example"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if rec := partnerReq(h, http.MethodPost, "/v1/webhooks", "k", body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestWebhooksDisabledWithoutKey(t *testing.T) {
	h := newTestServer(t, newFakeStore())
	if rec := partnerReq(h, http.MethodGet, "/v1/webhooks", "k", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}
