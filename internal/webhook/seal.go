package webhook

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// Sealer encrypts webhook signing secrets before they are stored in
// Postgres, so a database snapshot alone does not let anyone forge partner
// notifications. The 32-byte key lives in AWS Secrets Manager and reaches
// the API pods and the alert Lambda as an environment variable.
type Sealer struct {
	aead cipher.AEAD
}

// NewSealer builds a Sealer from a base64-encoded 32-byte key.
func NewSealer(b64Key string) (*Sealer, error) {
	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, fmt.Errorf("decode key: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("webhook secret key must be 32 bytes (AES-256)")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead}, nil
}

// Seal encrypts plaintext; the random nonce is prepended to the output.
// aad binds the ciphertext to its row (the subscription ID) so a ciphertext
// copied onto another row fails to open.
func (s *Sealer) Seal(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, plaintext, aad), nil
}

// Open decrypts a value produced by Seal.
func (s *Sealer) Open(ciphertext, aad []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(ciphertext) < n {
		return nil, errors.New("ciphertext too short")
	}
	return s.aead.Open(nil, ciphertext[:n], ciphertext[n:], aad)
}
