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

package ratify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

const (
	// defaultMaxBlobBytes is the default maximum size of the fetched blob
	// content.
	defaultMaxBlobBytes = 32 * 1024 * 1024 // 32 MiB

	// defaultMaxManifestBytes is the default maximum size of the fetched
	// manifest.
	defaultMaxManifestBytes = 4 * 1024 * 1024 // 4 MiB

	// defaultUserAgent is the default user agent string sent to the registry.
	defaultUserAgent = "ratify-go"

	// artifactTypeCosign is the artifact type for cosign signatures listed as
	// referrers.
	artifactTypeCosign = "application/vnd.dev.cosign.artifact.sig.v1+json"
)

// RegistryCredential represents the credential to access the registry.
type RegistryCredential = auth.Credential

// RegistryCredentialGetter provides the credential for the registry
// identified by the server address.
// This interface can be viewed as a read-only version of [credentials.Store].
type RegistryCredentialGetter interface {
	// Get returns the credential for the registry identified by the server
	// address.
	Get(ctx context.Context, serverAddress string) (RegistryCredential, error)
}

// RegistryStoreOptions provides options for creating a new [RegistryStore].
type RegistryStoreOptions struct {
	// HTTPClient is the HTTP client to use for the registry requests.
	// If nil, [http.DefaultClient] will be used.
	HTTPClient *http.Client

	// PlainHTTP signals the transport to access the remote repository via HTTP
	// instead of HTTPS.
	PlainHTTP bool

	// UserAgent is the user agent string to use for the registry requests.
	// If empty, "ratify-go" will be used.
	UserAgent string

	// CredentialProvider provides the credential for the registry.
	CredentialProvider RegistryCredentialGetter

	// MaxBlobBytes limits the maximum size of the fetched blob content bytes.
	// If less than or equal to zero, a default value (32 MiB) will be used.
	MaxBlobBytes int64

	// MaxManifestBytes limits the maximum size of the fetched manifest bytes.
	// If less than or equal to zero, a default value (4 MiB) will be used.
	MaxManifestBytes int64

	// AllowCosignTag enables fetching cosign signatures with the tag format
	// when listing referrers.
	AllowCosignTag bool
}

// RegistryStore is a store that interacts with a remote registry.
type RegistryStore struct {
	client           *auth.Client
	plainHTTP        bool
	maxBlobBytes     int64
	maxManifestBytes int64
	allowCosignTag   bool
}

// NewRegistryStore creates a new [RegistryStore] with options.
func NewRegistryStore(opts RegistryStoreOptions) *RegistryStore {
	client := &auth.Client{
		Client:   opts.HTTPClient,
		Cache:    auth.NewCache(),
		ClientID: "ratify-go",
	}
	if opts.UserAgent != "" {
		client.SetUserAgent(opts.UserAgent)
	} else {
		client.SetUserAgent(defaultUserAgent)
	}
	if opts.CredentialProvider != nil {
		client.Credential = credentials.Credential(readOnlyRegistryCredentialStore{opts.CredentialProvider})
	}

	maxBlobBytes := opts.MaxBlobBytes
	if maxBlobBytes <= 0 {
		maxBlobBytes = defaultMaxBlobBytes
	}
	maxManifestBytes := opts.MaxManifestBytes
	if maxManifestBytes <= 0 {
		maxManifestBytes = defaultMaxManifestBytes
	}

	return &RegistryStore{
		client:           client,
		plainHTTP:        opts.PlainHTTP,
		maxBlobBytes:     maxBlobBytes,
		maxManifestBytes: maxManifestBytes,
		allowCosignTag:   opts.AllowCosignTag,
	}
}

// Resolve resolves to a descriptor for the given artifact reference.
func (s *RegistryStore) Resolve(ctx context.Context, ref string) (ocispec.Descriptor, error) {
	repo, err := s.repository(ref)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	return repo.Manifests().Resolve(ctx, ref)
}

// ListReferrers returns the immediate set of supply chain artifacts for the
// given subject reference.
// Note: This API supports pagination. fn should be set to handle the
// underlying pagination.
// If AllowCosignTag was enabled in [RegistryStoreOptions], it will also attempt 
// to fetch cosign signatures with the sha256-<hash>.sig tag format.
func (s *RegistryStore) ListReferrers(ctx context.Context, ref string, artifactTypes []string, fn func(referrers []ocispec.Descriptor) error) error {
	repo, err := s.repository(ref)
	if err != nil {
		return err
	}

	// prepare the subject descriptor for calling the referrers API.
	// skip resolving the descriptor if the reference is a digest.
	var desc ocispec.Descriptor
	if subjectDigest, err := repo.Reference.Digest(); err == nil {
		desc = ocispec.Descriptor{
			Digest: subjectDigest,
		}
	} else {
		desc, err = repo.Manifests().Resolve(ctx, ref)
		if err != nil {
			// return empty referrer list if the subject is not found.
			// reference: https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#listing-referrers
			if errors.Is(err, errdef.ErrNotFound) {
				return nil
			}
			return err
		}
	}

	if s.allowCosignTag {
		if err := fetchCosignSignature(ctx, repo, desc, fn); err != nil {
			return err
		}
	}
	
	// fast path: optimize call to the referrers API for the artifact type
	// filter.
	if len(artifactTypes) == 1 {
		return repo.Referrers(ctx, desc, artifactTypes[0], fn)
	}

	// slow path: call the referrers API for all artifact types.
	return repo.Referrers(ctx, desc, "", func(referrers []ocispec.Descriptor) error {
		if len(artifactTypes) > 0 {
			referrers = slices.DeleteFunc(referrers, func(desc ocispec.Descriptor) bool {
				return !slices.Contains(artifactTypes, desc.ArtifactType)
			})
		}
		if len(referrers) == 0 {
			return nil
		}
		return fn(referrers)
	})
}

// FetchBlob returns the blob by the given reference.
// WARNING: This API is intended to use for small objects like signatures,
// SBoMs.
func (s *RegistryStore) FetchBlob(ctx context.Context, repoRef string, desc ocispec.Descriptor) ([]byte, error) {
	if desc.Size > s.maxBlobBytes {
		return nil, fmt.Errorf(
			"blob content size %v exceeds MaxBlobBytes %v: %w",
			desc.Size,
			s.maxBlobBytes,
			errdef.ErrSizeExceedsLimit)
	}

	repo, err := s.repository(repoRef)
	if err != nil {
		return nil, err
	}

	return content.FetchAll(ctx, repo.Blobs(), desc)
}

// FetchManifest returns the referenced manifest as given by the descriptor.
func (s *RegistryStore) FetchManifest(ctx context.Context, repoRef string, desc ocispec.Descriptor) ([]byte, error) {
	if desc.Size > s.maxManifestBytes {
		return nil, fmt.Errorf(
			"manifest size %v exceeds MaxManifestBytes %v: %w",
			desc.Size,
			s.maxManifestBytes,
			errdef.ErrSizeExceedsLimit)
	}

	repo, err := s.repository(repoRef)
	if err != nil {
		return nil, err
	}

	return content.FetchAll(ctx, repo.Manifests(), desc)
}

// repository returns a new remote repository for the given reference with the
// client set.
func (s *RegistryStore) repository(ref string) (*remote.Repository, error) {
	repo, err := remote.NewRepository(ref)
	if err != nil {
		return nil, err
	}
	repo.Client = s.client
	repo.PlainHTTP = s.plainHTTP
	return repo, nil
}

// readOnlyRegistryCredentialStore is wrapper around [RegistryCredentialGetter]
// to implement [credentials.Store] interface.
type readOnlyRegistryCredentialStore struct {
	RegistryCredentialGetter
}

func (readOnlyRegistryCredentialStore) Put(_ context.Context, _ string, _ RegistryCredential) error {
	return errors.New("registry credential: put: not supported")
}

func (readOnlyRegistryCredentialStore) Delete(_ context.Context, _ string) error {
	return errors.New("registry credential: delete: not supported")
}

// fetchCosignSignature attempts to fetch cosign signature for the given
// descriptor.
func fetchCosignSignature(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor, fn func(referrers []ocispec.Descriptor) error) error {
	sigTag := strings.ReplaceAll(desc.Digest.String(), ":", "-") + ".sig"

	sigDesc, err := repo.Manifests().Resolve(ctx, sigTag)
	if err != nil {
		// if signature is not found, it's not an error - just means no cosign
		// signature exists
		if errors.Is(err, errdef.ErrNotFound) {
			return nil
		}
		return err
	}

	sigDesc.ArtifactType = artifactTypeCosign

	return fn([]ocispec.Descriptor{sigDesc})
}
