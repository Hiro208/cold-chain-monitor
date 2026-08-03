// Package domain holds the core types of the cold-chain service and the pure
// business rules that operate on them. It has no I/O dependencies.
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Container is a refrigerated shipping container ("reefer") registered for
// monitoring. The acceptable temperature band is [MinTempC, MaxTempC].
type Container struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Commodity string    `json:"commodity"`
	SetpointC float64   `json:"setpoint_c"`
	MinTempC  float64   `json:"min_temp_c"`
	MaxTempC  float64   `json:"max_temp_c"`
	CreatedAt time.Time `json:"created_at"`
}

// Physical limits of typical reefer units. Values outside this range are
// almost certainly a typo in the registration request.
const (
	minSupportedTempC = -40.0
	maxSupportedTempC = 40.0
)

// ValidationError is returned for invalid user input. The API layer maps it to
// HTTP 400.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

// IsValidationError reports whether err (or anything it wraps) is a
// *ValidationError.
func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// Normalize trims whitespace and upper-cases the container ID in place.
func (c *Container) Normalize() {
	c.ID = strings.ToUpper(strings.TrimSpace(c.ID))
	c.Owner = strings.TrimSpace(c.Owner)
	c.Commodity = strings.TrimSpace(c.Commodity)
}

// Validate checks the registration fields.
func (c *Container) Validate() error {
	if err := ValidateContainerID(c.ID); err != nil {
		return err
	}
	if c.Owner == "" {
		return &ValidationError{Field: "owner", Reason: "required"}
	}
	if c.MinTempC >= c.MaxTempC {
		return &ValidationError{Field: "min_temp_c", Reason: "must be below max_temp_c"}
	}
	if c.MinTempC < minSupportedTempC || c.MaxTempC > maxSupportedTempC {
		return &ValidationError{Field: "temperature band", Reason: fmt.Sprintf("must lie within [%g, %g] °C", minSupportedTempC, maxSupportedTempC)}
	}
	if c.SetpointC < c.MinTempC || c.SetpointC > c.MaxTempC {
		return &ValidationError{Field: "setpoint_c", Reason: "must lie within the min/max band"}
	}
	return nil
}

// ValidateContainerID checks an ISO 6346 container number: 3-letter owner
// code, equipment category (U/J/Z), 6-digit serial and a check digit, e.g.
// "CSQU3054383".
func ValidateContainerID(id string) error {
	if len(id) != 11 {
		return &ValidationError{Field: "id", Reason: "must be 11 characters (ISO 6346)"}
	}
	for i := 0; i < 4; i++ {
		if id[i] < 'A' || id[i] > 'Z' {
			return &ValidationError{Field: "id", Reason: "first four characters must be letters"}
		}
	}
	if !strings.ContainsRune("UJZ", rune(id[3])) {
		return &ValidationError{Field: "id", Reason: "equipment category must be U, J or Z"}
	}
	for i := 4; i < 11; i++ {
		if id[i] < '0' || id[i] > '9' {
			return &ValidationError{Field: "id", Reason: "last seven characters must be digits"}
		}
	}
	if want := iso6346CheckDigit(id[:10]); int(id[10]-'0') != want {
		return &ValidationError{Field: "id", Reason: fmt.Sprintf("check digit mismatch (expected %d)", want)}
	}
	return nil
}

// iso6346CheckDigit computes the check digit for the first 10 characters of a
// container number. Letters map to 10..38 skipping multiples of 11.
func iso6346CheckDigit(prefix string) int {
	sum := 0
	for i := 0; i < len(prefix); i++ {
		sum += iso6346Value(prefix[i]) << i // weight 2^i
	}
	return sum % 11 % 10
}

func iso6346Value(ch byte) int {
	if ch >= '0' && ch <= '9' {
		return int(ch - '0')
	}
	v := 10
	for c := byte('A'); c < ch; c++ {
		v++
		if v%11 == 0 {
			v++
		}
	}
	return v
}

// MakeContainerID builds a valid ISO 6346 number from a 3-letter owner code
// and serial. Used by the simulator and tests.
func MakeContainerID(owner string, serial int) string {
	prefix := fmt.Sprintf("%sU%06d", strings.ToUpper(owner), serial%1_000_000)
	return fmt.Sprintf("%s%d", prefix, iso6346CheckDigit(prefix))
}
