package webhook

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_780_000_000, 0)
	body := []byte(`{"type":"breach.opened"}`)
	hdr := Sign(secret, now, body)

	if err := Verify(secret, hdr, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := Verify(secret, hdr, []byte(`{"type":"breach.closed"}`), now, 5*time.Minute); err == nil {
		t.Fatal("tampered body accepted")
	}
	if err := Verify("whsec_other", hdr, body, now, 5*time.Minute); err == nil {
		t.Fatal("wrong secret accepted")
	}
	if err := Verify(secret, hdr, body, now.Add(time.Hour), 5*time.Minute); err == nil {
		t.Fatal("replayed signature accepted")
	}
	if err := Verify(secret, "garbage", body, now, 5*time.Minute); err == nil {
		t.Fatal("malformed header accepted")
	}
}

func TestSealer(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s, err := NewSealer(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	ct, err := s.Seal([]byte("whsec_abc"), []byte("sub-1"))
	if err != nil {
		t.Fatal(err)
	}
	pt, err := s.Open(ct, []byte("sub-1"))
	if err != nil || string(pt) != "whsec_abc" {
		t.Fatalf("open = %q, %v", pt, err)
	}
	if _, err := s.Open(ct, []byte("sub-2")); err == nil {
		t.Fatal("ciphertext moved to another row must not decrypt")
	}
	if _, err := NewSealer(base64.StdEncoding.EncodeToString(key[:16])); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestValidateURL(t *testing.T) {
	ok := []string{"https://partner.example.com/hooks/cold"}
	bad := []string{
		"http://partner.example.com/x",
		"https://127.0.0.1/x",
		"https://169.254.169.254/latest/meta-data",
		"https://10.0.0.5/x",
		"https://user:pw@partner.example.com/x",
		"ftp://partner.example.com",
		"https:///nohost",
	}
	for _, u := range ok {
		if err := ValidateURL(u, false); err != nil {
			t.Errorf("%s rejected: %v", u, err)
		}
	}
	for _, u := range bad {
		if err := ValidateURL(u, false); err == nil {
			t.Errorf("%s accepted", u)
		}
	}
}

func TestDeliverSignsAndRetries(t *testing.T) {
	var calls atomic.Int32
	secret := "whsec_test"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := Verify(secret, r.Header.Get(SignatureHeader), body, time.Now(), time.Minute); err != nil {
			t.Errorf("bad signature: %v", err)
		}
		if r.Header.Get(EventIDHeader) != "42:breach.opened" {
			t.Errorf("event id = %q", r.Header.Get(EventIDHeader))
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDeliverer(AllowPrivateTargets(), WithRetry(3, time.Millisecond))
	err := d.Deliver(context.Background(), Delivery{URL: srv.URL, Secret: secret, EventID: "42:breach.opened", Body: []byte(`{}`)})
	if err != nil || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestDeliverDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()
	d := NewDeliverer(AllowPrivateTargets(), WithRetry(5, time.Millisecond))
	err := d.Deliver(context.Background(), Delivery{URL: srv.URL, Secret: "s", Body: []byte(`{}`)})
	var perm *PermanentError
	if !errors.As(err, &perm) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestDeliverBlocksLoopbackByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("guarded client reached a loopback server")
	}))
	defer srv.Close()
	err := NewDeliverer(WithRetry(1, 0)).Deliver(context.Background(), Delivery{URL: srv.URL, Secret: "s", Body: []byte(`{}`)})
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("err = %v, want ErrBlockedAddress", err)
	}
}
