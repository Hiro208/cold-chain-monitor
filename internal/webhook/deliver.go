package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

// ErrBlockedAddress is returned when a webhook resolves to a private,
// loopback or link-local address. Partners supply the URL, so without this
// check a subscription could be used to probe the VPC (SSRF), e.g. the EC2
// metadata endpoint at 169.254.169.254.
var ErrBlockedAddress = errors.New("webhook target resolves to a non-public address")

// ValidateURL performs static checks on a partner-supplied URL.
func ValidateURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "https" && !(allowInsecure && u.Scheme == "http") {
		return errors.New("url must use https")
	}
	if u.Hostname() == "" {
		return errors.New("url must include a host")
	}
	if u.User != nil {
		return errors.New("url must not contain credentials")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !isPublic(ip) {
		return ErrBlockedAddress
	}
	return nil
}

func isPublic(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast())
}

// Deliverer POSTs signed payloads to partner endpoints.
type Deliverer struct {
	client   *http.Client
	attempts int
	backoff  time.Duration
	now      func() time.Time
}

// DelivererOption customises a Deliverer.
type DelivererOption func(*Deliverer)

// AllowPrivateTargets disables the SSRF guard (tests and local dev only).
func AllowPrivateTargets() DelivererOption {
	return func(d *Deliverer) { d.client = newClient(false) }
}

// WithRetry sets attempts and the initial backoff.
func WithRetry(attempts int, backoff time.Duration) DelivererOption {
	return func(d *Deliverer) { d.attempts, d.backoff = attempts, backoff }
}

// NewDeliverer returns a Deliverer with a 5 s per-attempt timeout and the
// SSRF guard enabled.
func NewDeliverer(opts ...DelivererOption) *Deliverer {
	d := &Deliverer{client: newClient(true), attempts: 3, backoff: 500 * time.Millisecond, now: time.Now}
	for _, o := range opts {
		o(d)
	}
	return d
}

func newClient(guard bool) *http.Client {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	if guard {
		// Check the address actually dialled, after DNS resolution, which
		// also defeats DNS-rebinding tricks.
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip := net.ParseIP(host); ip == nil || !isPublic(ip) {
				return ErrBlockedAddress
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: 3 * time.Second,
			MaxIdleConnsPerHost: 4,
		},
		// Do not follow redirects: a redirect could point at an internal host.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Delivery is one webhook call.
type Delivery struct {
	URL     string
	Secret  string
	EventID string
	Body    []byte
}

// PermanentError marks a failure that retrying will not fix (4xx other than
// 408/429, blocked address).
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Deliver sends d, retrying transient failures with exponential backoff.
func (dl *Deliverer) Deliver(ctx context.Context, d Delivery) error {
	backoff := dl.backoff
	var last error
	for attempt := 1; attempt <= dl.attempts; attempt++ {
		last = dl.once(ctx, d)
		var perm *PermanentError
		if last == nil || errors.As(last, &perm) {
			return last
		}
		if attempt < dl.attempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
	return fmt.Errorf("after %d attempts: %w", dl.attempts, last)
}

func (dl *Deliverer) once(ctx context.Context, d Delivery) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		return &PermanentError{err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "coldchain-webhooks/1")
	req.Header.Set(EventIDHeader, d.EventID)
	req.Header.Set(SignatureHeader, Sign(d.Secret, dl.now(), d.Body))

	resp, err := dl.client.Do(req)
	if err != nil {
		if errors.Is(err, ErrBlockedAddress) {
			return &PermanentError{err}
		}
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return fmt.Errorf("partner returned %d", resp.StatusCode)
	default:
		return &PermanentError{fmt.Errorf("partner returned %d", resp.StatusCode)}
	}
}
