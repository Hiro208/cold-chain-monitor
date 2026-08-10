package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

// maxBodyBytes caps request bodies; registration payloads are tiny.
const maxBodyBytes = 64 << 10

// errorBody is the uniform error envelope returned by every endpoint.
type errorBody struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("encode response", "err", err)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg, RequestID: requestIDFrom(r.Context())})
}

// writeErr maps domain and store errors to HTTP statuses. Unexpected errors
// are logged and surfaced as a generic 500 so internals never leak.
func writeErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case domain.IsValidationError(err):
		writeError(w, r, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, r, http.StatusConflict, "already exists")
	default:
		loggerFrom(r.Context()).Error("request failed", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
	}
}

// decodeJSON strictly decodes a single JSON object from the request body.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &domain.ValidationError{Field: "body", Reason: err.Error()}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &domain.ValidationError{Field: "body", Reason: "must contain a single JSON object"}
	}
	return nil
}
