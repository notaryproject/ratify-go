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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

func TestRegistryStore(t *testing.T) {
	var store any = &RegistryStore{}
	if _, ok := store.(Store); !ok {
		t.Error("*RegistryStore does not implement Store")
	}
}

func TestNewRegistryStore(t *testing.T) {
	t.Run("create store with default settings", func(t *testing.T) {
		s := NewRegistryStore(RegistryStoreOptions{})
		if s.client.Client != nil {
			t.Errorf("RegistryStore.client.Client = %v, want nil", s.client.Client)
		}
		if s.plainHTTP != false {
			t.Errorf("RegistryStore.plainHTTP = %v, want false", s.plainHTTP)
		}
		if userAgent := s.client.Header.Get("User-Agent"); userAgent != defaultUserAgent {
			t.Errorf(`RegistryStore.client.Header["User-Agent"] = %v, want %v`, userAgent, defaultUserAgent)
		}
		if credential := s.client.Credential; credential != nil {
			t.Errorf("RegistryStore.client.Credential = %v, want nil", credential)
		}
		if s.maxBlobBytes != defaultMaxBlobBytes {
			t.Errorf("RegistryStore.maxBlobBytes = %v, want %v", s.maxBlobBytes, defaultMaxBlobBytes)
		}
		if s.maxManifestBytes != defaultMaxManifestBytes {
			t.Errorf("RegistryStore.maxManifestBytes = %v, want %v", s.maxManifestBytes, defaultMaxManifestBytes)
		}
	})

	t.Run("create store with custom settings", func(t *testing.T) {
		opts := RegistryStoreOptions{
			HTTPClient:         &http.Client{},
			PlainHTTP:          true,
			UserAgent:          "test-agent",
			CredentialProvider: credentials.NewMemoryStore(),
			MaxBlobBytes:       2048,
			MaxManifestBytes:   1024,
		}
		s := NewRegistryStore(opts)
		if s.client.Client != opts.HTTPClient {
			t.Errorf("RegistryStore.client.Client = %v, want %v", s.client.Client, opts.HTTPClient)
		}
		if s.plainHTTP != opts.PlainHTTP {
			t.Errorf("RegistryStore.plainHTTP = %v, want %v", s.plainHTTP, opts.PlainHTTP)
		}
		if userAgent := s.client.Header.Get("User-Agent"); userAgent != opts.UserAgent {
			t.Errorf(`RegistryStore.client.Header["User-Agent"] = %v, want %v`, userAgent, opts.UserAgent)
		}
		if credential := s.client.Credential; credential == nil {
			t.Errorf("RegistryStore.client.Credential = nil, want non-nil")
		}
		if s.maxBlobBytes != opts.MaxBlobBytes {
			t.Errorf("RegistryStore.maxBlobBytes = %v, want %v", s.maxBlobBytes, opts.MaxBlobBytes)
		}
		if s.maxManifestBytes != opts.MaxManifestBytes {
			t.Errorf("RegistryStore.maxManifestBytes = %v, want %v", s.maxManifestBytes, opts.MaxManifestBytes)
		}
	})
}

func TestRegistryStore_Resolve(t *testing.T) {
	blob := []byte("hello world")
	blobDesc := content.NewDescriptorFromBytes("test", blob)
	index := []byte(`{"manifests":[]}`)
	indexDesc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, index)
	ref := "v1"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("unexpected access: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/v2/test/manifests/" + blobDesc.Digest.String(),
			"/v2/test/manifests/v2":
			w.WriteHeader(http.StatusNotFound)
		case "/v2/test/manifests/" + indexDesc.Digest.String(),
			"/v2/test/manifests/" + ref:
			if accept := r.Header.Get("Accept"); !strings.Contains(accept, indexDesc.MediaType) {
				t.Errorf("manifest not convertible: %s", accept)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", indexDesc.MediaType)
			w.Header().Set("Docker-Content-Digest", indexDesc.Digest.String())
			w.Header().Set("Content-Length", strconv.Itoa(int(indexDesc.Size)))
		default:
			t.Errorf("unexpected access: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	repoName := uri.Host + "/test"
	store := NewRegistryStore(RegistryStoreOptions{
		PlainHTTP: true,
	})
	ctx := context.Background()

	tests := []struct {
		name    string
		ref     string
		want    ocispec.Descriptor
		wantErr bool
	}{
		{
			name: "tag ref",
			ref:  repoName + ":v1",
			want: indexDesc,
		},
		{
			name: "digest ref",
			ref:  repoName + "@" + indexDesc.Digest.String(),
			want: indexDesc,
		},
		{
			name:    "non-existing ref",
			ref:     repoName + ":v2",
			wantErr: true,
		},
		{
			name:    "non-existing digest ref",
			ref:     repoName + "@" + blobDesc.Digest.String(),
			wantErr: true,
		},
		{
			name:    "invalid ref",
			ref:     "invalid",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Resolve(ctx, tt.ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegistryStore.Resolve() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegistryStore.Resolve() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegistryStore_ListReferrers(t *testing.T) {
	manifest := []byte(`{"layers":[]}`)
	manifestDesc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, manifest)
	referrerSet := [][]ocispec.Descriptor{
		{
			{
				MediaType:    ocispec.MediaTypeImageManifest,
				Size:         1,
				Digest:       digest.FromString("1"),
				ArtifactType: "application/foo",
			},
			{
				MediaType:    ocispec.MediaTypeImageManifest,
				Size:         2,
				Digest:       digest.FromString("2"),
				ArtifactType: "application/bar",
			},
		},
		{
			{
				MediaType:    ocispec.MediaTypeImageManifest,
				Size:         3,
				Digest:       digest.FromString("3"),
				ArtifactType: "application/foo",
			},
			{
				MediaType:    ocispec.MediaTypeImageManifest,
				Size:         4,
				Digest:       digest.FromString("4"),
				ArtifactType: "application/bar",
			},
		},
		{
			{
				MediaType:    ocispec.MediaTypeImageManifest,
				Size:         5,
				Digest:       digest.FromString("5"),
				ArtifactType: "application/foo",
			},
		},
	}
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			switch r.URL.Path {
			case "/v2/test/manifests/v1":
				if accept := r.Header.Get("Accept"); !strings.Contains(accept, manifestDesc.MediaType) {
					t.Errorf("manifest not convertible: %s", accept)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", manifestDesc.MediaType)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Header().Set("Content-Length", strconv.Itoa(int(manifestDesc.Size)))
				return
			case "/v2/test/manifests/v2":
				w.WriteHeader(http.StatusNotFound)
				return
			default:
				// Handle cosign signature requests (*.sig tags)
				if strings.HasSuffix(r.URL.Path, ".sig") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}
		}

		path := "/v2/test/referrers/" + manifestDesc.Digest.String()
		if r.Method != http.MethodGet || r.URL.Path != path {
			referrersTag := strings.Replace(manifestDesc.Digest.String(), ":", "-", 1)
			if r.URL.Path != "/v2/test/manifests/"+referrersTag {
				t.Errorf("unexpected access: %s %q", r.Method, r.URL)
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		q := r.URL.Query()
		var referrers []ocispec.Descriptor
		switch artifactType := q.Get("artifactType"); artifactType {
		case "":
			switch q.Get("test") {
			case "foo":
				referrers = referrerSet[1]
				w.Header().Set("Link", fmt.Sprintf(`<%s%s?n=2&test=bar>; rel="next"`, ts.URL, path))
			case "bar":
				referrers = referrerSet[2]
			default:
				referrers = referrerSet[0]
				w.Header().Set("Link", fmt.Sprintf(`<%s?n=2&test=foo>; rel="next"`, path))
			}
		case "application/foo":
			referrers = []ocispec.Descriptor{
				referrerSet[0][0],
				referrerSet[1][0],
				referrerSet[2][0],
			}
		default:
			referrers = []ocispec.Descriptor{}
		}
		result := ocispec.Index{
			Versioned: specs.Versioned{
				SchemaVersion: 2, // historical value. does not pertain to OCI or docker version
			},
			MediaType: ocispec.MediaTypeImageIndex,
			Manifests: referrers,
		}
		w.Header().Set("Content-Type", ocispec.MediaTypeImageIndex)
		if err := json.NewEncoder(w).Encode(result); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer ts.Close()
	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	repoName := uri.Host + "/test"
	store := NewRegistryStore(RegistryStoreOptions{
		PlainHTTP:      true,
		AllowCosignTag: true,
	})
	ctx := context.Background()

	tests := []struct {
		name          string
		ref           string
		artifactTypes []string
		want          []ocispec.Descriptor
		wantFnCount   int32
		wantErr       bool
	}{
		{
			name:        "list all referrers",
			ref:         repoName + ":v1",
			want:        slices.Concat(referrerSet...),
			wantFnCount: 3,
		},
		{
			name:        "list all referrers of a digest reference",
			ref:         repoName + "@" + manifestDesc.Digest.String(),
			want:        slices.Concat(referrerSet...),
			wantFnCount: 3,
		},
		{
			name:          "list referrers of certain type",
			ref:           repoName + ":v1",
			artifactTypes: []string{"application/foo"},
			want: []ocispec.Descriptor{
				referrerSet[0][0],
				referrerSet[1][0],
				referrerSet[2][0],
			},
			wantFnCount: 1,
		},
		{
			name:          "list referrers of non-existing type",
			ref:           repoName + ":v1",
			artifactTypes: []string{"application/test"},
			want:          nil,
		},
		{
			name:          "list referrers of partial non-existing type",
			ref:           repoName + ":v1",
			artifactTypes: []string{"application/foo", "application/test"},
			want: []ocispec.Descriptor{
				referrerSet[0][0],
				referrerSet[1][0],
				referrerSet[2][0],
			},
			wantFnCount: 3,
		},
		{
			name: "non-existing ref",
			ref:  repoName + ":v2",
			want: nil,
		},
		{
			name:    "invalid ref",
			ref:     "invalid",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []ocispec.Descriptor
			var fnCount int32
			fn := func(referrers []ocispec.Descriptor) error {
				atomic.AddInt32(&fnCount, 1)
				got = append(got, referrers...)
				return nil
			}
			if err := store.ListReferrers(ctx, tt.ref, tt.artifactTypes, fn); (err != nil) != tt.wantErr {
				t.Errorf("RegistryStore.ListReferrers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if fnCount != tt.wantFnCount {
				t.Errorf("RegistryStore.ListReferrers() count(fn) = %v, want %v", fnCount, tt.wantFnCount)
			}
			slices.SortFunc(got, func(a, b ocispec.Descriptor) int {
				return int(a.Size - b.Size)
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OCIStore.ListReferrers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegistryStore_FetchBlob(t *testing.T) {
	blob := []byte("hello world")
	blobDesc := content.NewDescriptorFromBytes("test", blob)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected access: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/v2/test/blobs/" + blobDesc.Digest.String():
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", blobDesc.Digest.String())
			if _, err := w.Write(blob); err != nil {
				t.Errorf("failed to write %q: %v", r.URL, err)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	repoName := uri.Host + "/test"
	store := NewRegistryStore(RegistryStoreOptions{
		PlainHTTP: true,
	})
	ctx := context.Background()

	tests := []struct {
		name    string
		repo    string
		desc    ocispec.Descriptor
		want    []byte
		wantErr bool
	}{
		{
			name: "fetch blob",
			repo: repoName,
			desc: blobDesc,
			want: blob,
		},
		{
			name: "non-existing blob",
			repo: repoName,
			desc: ocispec.Descriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Size:      0,
			},
			wantErr: true,
		},
		{
			name:    "non-existing repo",
			repo:    uri.Host + "/test2",
			desc:    blobDesc,
			wantErr: true,
		},
		{
			name:    "invalid repo",
			repo:    "invalid",
			desc:    blobDesc,
			wantErr: true,
		},
		{
			name:    "blob too large",
			repo:    repoName,
			desc:    ocispec.Descriptor{Size: defaultMaxBlobBytes + 1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.FetchBlob(ctx, tt.repo, tt.desc)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegistryStore.FetchBlob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegistryStore.FetchBlob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegistryStore_FetchManifest(t *testing.T) {
	manifest := &ocispec.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: "application/vnd.unknown.artifact.v1",
		Config:       ocispec.DescriptorEmptyJSON,
		Layers: []ocispec.Descriptor{
			{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447",
				Size:      12,
				Annotations: map[string]string{
					ocispec.AnnotationTitle: "hello.txt",
				},
			},
		},
		Annotations: map[string]string{
			ocispec.AnnotationCreated: "2025-01-22T09:54:41Z",
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	manifestDesc := content.NewDescriptorFromBytes(manifest.MediaType, manifestBytes)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected access: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/v2/test/manifests/" + manifestDesc.Digest.String():
			if accept := r.Header.Get("Accept"); !strings.Contains(accept, manifestDesc.MediaType) {
				t.Errorf("manifest not convertible: %s", accept)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", manifestDesc.MediaType)
			w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
			if _, err := w.Write(manifestBytes); err != nil {
				t.Errorf("failed to write %q: %v", r.URL, err)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	repoName := uri.Host + "/test"
	store := NewRegistryStore(RegistryStoreOptions{
		PlainHTTP: true,
	})
	ctx := context.Background()

	tests := []struct {
		name    string
		repo    string
		desc    ocispec.Descriptor
		want    []byte
		wantErr bool
	}{
		{
			name: "fetch manifest",
			repo: repoName,
			desc: manifestDesc,
			want: manifestBytes,
		},
		{
			name: "non-existing manifest",
			repo: repoName,
			desc: ocispec.Descriptor{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Size:      0,
			},
			wantErr: true,
		},
		{
			name:    "non-existing repo",
			repo:    uri.Host + "/test2",
			desc:    manifestDesc,
			wantErr: true,
		},
		{
			name:    "invalid repo",
			repo:    "invalid",
			desc:    manifestDesc,
			wantErr: true,
		},
		{
			name:    "manifest too large",
			repo:    repoName,
			desc:    ocispec.Descriptor{Size: defaultMaxManifestBytes + 1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.FetchManifest(ctx, tt.repo, tt.desc)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegistryStore.FetchManifest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegistryStore.FetchManifest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegistryStore_AllowCosignTag(t *testing.T) {
	t.Run("cosign signature disabled by default", func(t *testing.T) {
		store := NewRegistryStore(RegistryStoreOptions{})
		if store.allowCosignTag {
			t.Error("AllowCosignTag should be false by default")
		}
	})

	t.Run("cosign signature enabled when configured", func(t *testing.T) {
		store := NewRegistryStore(RegistryStoreOptions{
			AllowCosignTag: true,
		})
		if !store.allowCosignTag {
			t.Error("AllowCosignTag should be true when configured")
		}
	})
}

func TestRegistryStore_fetchCosignSignature(t *testing.T) {
	// Create a sample descriptor
	manifest := []byte(`{"layers":[]}`)
	manifestDesc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, manifest)

	// Create a sample cosign signature descriptor
	cosignSig := []byte(`{"critical":{"identity":{"docker-reference":"test"}}}`)
	cosignSigDesc := content.NewDescriptorFromBytes("application/vnd.dev.cosign.simplesigning.v1+json", cosignSig)

	t.Run("cosign signature found", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodHead {
				t.Errorf("unexpected method: %s, want HEAD", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// Expected cosign signature tag format: sha256-<hash>.sig
			expectedSigTag := strings.ReplaceAll(manifestDesc.Digest.String(), ":", "-") + ".sig"
			expectedPath := "/v2/test/manifests/" + expectedSigTag

			if r.URL.Path != expectedPath {
				t.Errorf("unexpected path: %s, want %s", r.URL.Path, expectedPath)
				w.WriteHeader(http.StatusNotFound)
				return
			}

			// Return the cosign signature descriptor
			w.Header().Set("Content-Type", cosignSigDesc.MediaType)
			w.Header().Set("Docker-Content-Digest", cosignSigDesc.Digest.String())
			w.Header().Set("Content-Length", strconv.Itoa(int(cosignSigDesc.Size)))
			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()

		uri, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatalf("invalid test http server: %v", err)
		}

		store := NewRegistryStore(RegistryStoreOptions{
			PlainHTTP:      true,
			AllowCosignTag: true,
		})

		repo, err := store.repository(uri.Host + "/test:tag")
		if err != nil {
			t.Fatalf("failed to create repository: %v", err)
		}

		ctx := context.Background()
		var receivedReferrers []ocispec.Descriptor
		fn := func(referrers []ocispec.Descriptor) error {
			receivedReferrers = referrers
			return nil
		}

		err = fetchCosignSignature(ctx, repo, manifestDesc, fn)
		if err != nil {
			t.Errorf("fetchCosignSignature() error = %v, want nil", err)
		}

		if len(receivedReferrers) != 1 {
			t.Fatalf("expected 1 referrer, got %d", len(receivedReferrers))
		}

		referrer := receivedReferrers[0]
		expectedReferrer := ocispec.Descriptor{
			MediaType:    cosignSigDesc.MediaType,
			Digest:       cosignSigDesc.Digest,
			Size:         cosignSigDesc.Size,
			ArtifactType: "application/vnd.dev.cosign.artifact.sig.v1+json",
		}

		if !reflect.DeepEqual(referrer, expectedReferrer) {
			t.Errorf("received referrer = %v, want %v", referrer, expectedReferrer)
		}
	})

	t.Run("cosign signature not found", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodHead {
				t.Errorf("unexpected method: %s, want HEAD", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// Return 404 for any cosign signature request
			w.WriteHeader(http.StatusNotFound)
		}))
		defer ts.Close()

		uri, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatalf("invalid test http server: %v", err)
		}

		store := NewRegistryStore(RegistryStoreOptions{
			PlainHTTP:      true,
			AllowCosignTag: true,
		})

		repo, err := store.repository(uri.Host + "/test:tag")
		if err != nil {
			t.Fatalf("failed to create repository: %v", err)
		}

		ctx := context.Background()
		called := false
		fn := func(referrers []ocispec.Descriptor) error {
			called = true
			return nil
		}

		err = fetchCosignSignature(ctx, repo, manifestDesc, fn)
		if err != nil {
			t.Errorf("fetchCosignSignature() error = %v, want nil", err)
		}
		if called {
			t.Error("callback function should not be called when cosign signature is not found")
		}
	})

	t.Run("registry error during cosign signature fetch", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodHead {
				t.Errorf("unexpected method: %s, want HEAD", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// Return a server error
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		uri, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatalf("invalid test http server: %v", err)
		}

		store := NewRegistryStore(RegistryStoreOptions{
			PlainHTTP:      true,
			AllowCosignTag: true,
		})

		repo, err := store.repository(uri.Host + "/test:tag")
		if err != nil {
			t.Fatalf("failed to create repository: %v", err)
		}

		ctx := context.Background()
		fn := func(referrers []ocispec.Descriptor) error {
			return nil
		}

		err = fetchCosignSignature(ctx, repo, manifestDesc, fn)
		if err == nil {
			t.Error("fetchCosignSignature() error = nil, want non-nil error")
		}
	})

	t.Run("callback function returns error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodHead {
				t.Errorf("unexpected method: %s, want HEAD", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// Return the cosign signature descriptor
			w.Header().Set("Content-Type", cosignSigDesc.MediaType)
			w.Header().Set("Docker-Content-Digest", cosignSigDesc.Digest.String())
			w.Header().Set("Content-Length", strconv.Itoa(int(cosignSigDesc.Size)))
			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()

		uri, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatalf("invalid test http server: %v", err)
		}

		store := NewRegistryStore(RegistryStoreOptions{
			PlainHTTP:      true,
			AllowCosignTag: true,
		})

		repo, err := store.repository(uri.Host + "/test:tag")
		if err != nil {
			t.Fatalf("failed to create repository: %v", err)
		}

		ctx := context.Background()
		expectedErr := fmt.Errorf("callback error")
		fn := func(referrers []ocispec.Descriptor) error {
			return expectedErr
		}

		err = fetchCosignSignature(ctx, repo, manifestDesc, fn)
		if err != expectedErr {
			t.Errorf("fetchCosignSignature() error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("cosign signature tag format verification", func(t *testing.T) {
		// Test with various digest formats to ensure proper tag generation
		testDigests := []digest.Digest{
			digest.FromString("test1"),
			digest.FromString("test2"),
			digest.FromBytes([]byte("test3")),
		}

		for _, testDigest := range testDigests {
			testDesc := ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageManifest,
				Digest:    testDigest,
				Size:      100,
			}

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					t.Errorf("unexpected method: %s, want HEAD", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}

				// Verify the cosign signature tag format
				expectedSigTag := strings.ReplaceAll(testDigest.String(), ":", "-") + ".sig"
				expectedPath := "/v2/test/manifests/" + expectedSigTag

				if r.URL.Path != expectedPath {
					t.Errorf("unexpected path for digest %s: %s, want %s", testDigest, r.URL.Path, expectedPath)
					w.WriteHeader(http.StatusNotFound)
					return
				}

				// Return the cosign signature descriptor
				w.Header().Set("Content-Type", cosignSigDesc.MediaType)
				w.Header().Set("Docker-Content-Digest", cosignSigDesc.Digest.String())
				w.Header().Set("Content-Length", strconv.Itoa(int(cosignSigDesc.Size)))
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			uri, err := url.Parse(ts.URL)
			if err != nil {
				t.Fatalf("invalid test http server: %v", err)
			}

			store := NewRegistryStore(RegistryStoreOptions{
				PlainHTTP:      true,
				AllowCosignTag: true,
			})

			repo, err := store.repository(uri.Host + "/test:tag")
			if err != nil {
				t.Fatalf("failed to create repository: %v", err)
			}

			ctx := context.Background()
			called := false
			fn := func(referrers []ocispec.Descriptor) error {
				called = true
				return nil
			}

			err = fetchCosignSignature(ctx, repo, testDesc, fn)
			if err != nil {
				t.Errorf("fetchCosignSignature() error = %v, want nil for digest %s", err, testDigest)
			}
			if !called {
				t.Errorf("callback function should be called for digest %s", testDigest)
			}
		}
	})
}
