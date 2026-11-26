/*
Copyright The Ratify Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugin

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
	"time"
)

// PublicKey is a public key returned by a [KeyProvider].
type PublicKey struct {
	// Key for key-based verification. Required.
	Key crypto.PublicKey

	// SignatureAlgorithm defines the algorithm used for signature verification.
	// Optional. If not provided, defaults to SHA256.
	SignatureAlgorithm crypto.Hash

	// ValidityPeriodStart defines the start time for the public key validity
	// period. Optional. If not provided, the key is considered valid from the
	// beginning of time.
	ValidityPeriodStart time.Time

	// ValidityPeriodEnd defines the end time for the public key validity
	// period. Optional. If not provided, the key is considered valid until the
	// end of time.
	ValidityPeriodEnd time.Time
}

// KeyProvider provides crypto material for signature verification. Key
// providers are shared by verifier plugins: for example, both the Notation and
// the Cosign verifiers can load trust anchors from the same Azure Key Vault
// key provider.
type KeyProvider interface {
	// GetCertificates fetches certificates for certificate-based verification.
	GetCertificates(ctx context.Context) ([]*x509.Certificate, error)

	// GetKeys fetches public keys for key-based verification.
	GetKeys(ctx context.Context) ([]*PublicKey, error)
}

// DecodeParameters decodes raw plugin parameters into v. Empty and null
// parameters leave v unchanged.
func DecodeParameters(raw json.RawMessage, v any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return json.Unmarshal(trimmed, v)
}
