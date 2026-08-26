// Package webhook implements partner webhook delivery: payload signing,
// encryption of signing secrets at rest, and an SSRF-safe HTTP deliverer.
package webhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader carries "t=<unix>,v1=<hex hmac>". The timestamp is part of
// the signed content so a captured request cannot be replayed later.
const SignatureHeader = "X-Coldchain-Signature"

// EventIDHeader lets receivers dedupe at-least-once deliveries.
const EventIDHeader = "X-Coldchain-Event-Id"

// NewSecret returns a random signing secret for a new subscription.
func NewSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "whsec_" + base64.RawURLEncoding.EncodeToString(b), nil
}

// Sign returns the signature header value for body at time ts.
func Sign(secret string, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + mac(secret, t, body)
}

func mac(secret, t string, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(t))
	h.Write([]byte{'.'})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// Verify checks a signature header, rejecting it if older than tolerance.
// Partners can use the same algorithm; it is published in the API docs.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var t, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(part, "=")
		switch k {
		case "t":
			t = v
		case "v1":
			sig = v
		}
	}
	if t == "" || sig == "" {
		return errors.New("malformed signature header")
	}
	unix, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return fmt.Errorf("bad timestamp: %w", err)
	}
	if age := now.Sub(time.Unix(unix, 0)); age > tolerance || age < -tolerance {
		return errors.New("signature timestamp outside tolerance")
	}
	if !hmac.Equal([]byte(sig), []byte(mac(secret, t, body))) {
		return errors.New("signature mismatch")
	}
	return nil
}
