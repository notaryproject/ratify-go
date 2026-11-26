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
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/notaryproject/ratify-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type testVerifier struct {
	name   string
	params map[string]string
	scopes []string
	keys   KeyProvider
}

func (v *testVerifier) Name() string                         { return v.name }
func (v *testVerifier) Type() string                         { return "test" }
func (v *testVerifier) Verifiable(_ ocispec.Descriptor) bool { return true }
func (v *testVerifier) Verify(_ context.Context, _ *ratify.VerifyOptions) (*ratify.VerificationResult, error) {
	return &ratify.VerificationResult{}, nil
}

type testKeyProvider struct{ id string }

func (p *testKeyProvider) GetCertificates(_ context.Context) ([]*x509.Certificate, error) {
	return nil, nil
}
func (p *testKeyProvider) GetKeys(_ context.Context) ([]*PublicKey, error) { return nil, nil }

type testStore struct{ ratify.Store }

type testPolicyEnforcer struct{ ratify.PolicyEnforcer }

func newTestVerifier(ctx context.Context, opts VerifierOptions) (ratify.Verifier, error) {
	var params struct {
		Values      map[string]string `json:"values"`
		KeyProvider string            `json:"keyProvider"`
	}
	if err := DecodeParameters(opts.Parameters, &params); err != nil {
		return nil, err
	}
	v := &testVerifier{name: opts.Name, params: params.Values, scopes: opts.Scopes}
	if params.KeyProvider != "" {
		kp, err := opts.Registry.NewKeyProvider(ctx, params.KeyProvider, KeyProviderOptions{})
		if err != nil {
			return nil, err
		}
		v.keys = kp
	}
	return v, nil
}

func TestRegistry_Verifier(t *testing.T) {
	r := NewRegistry()
	if err := r.RegisterVerifier("test", newTestVerifier); err != nil {
		t.Fatalf("RegisterVerifier() error = %v", err)
	}
	if err := r.RegisterKeyProvider("kp", func(_ context.Context, _ KeyProviderOptions) (KeyProvider, error) {
		return &testKeyProvider{id: "kp"}, nil
	}); err != nil {
		t.Fatalf("RegisterKeyProvider() error = %v", err)
	}

	v, err := r.NewVerifier(context.Background(), "test", VerifierOptions{
		Name:       "v1",
		Parameters: json.RawMessage(`{"values":{"a":"b"},"keyProvider":"kp"}`),
		Scopes:     []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewVerifier() error = %v", err)
	}
	tv := v.(*testVerifier)
	if tv.name != "v1" || tv.params["a"] != "b" || !reflect.DeepEqual(tv.scopes, []string{"example.com"}) {
		t.Fatalf("unexpected verifier: %+v", tv)
	}
	if kp, ok := tv.keys.(*testKeyProvider); !ok || kp.id != "kp" {
		t.Fatalf("nested key provider was not resolved from the same registry: %+v", tv.keys)
	}
}

func TestRegistry_Errors(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	if err := r.RegisterVerifier("", newTestVerifier); err == nil {
		t.Error("expected error for empty type")
	}
	if err := r.RegisterVerifier("nil", nil); err == nil {
		t.Error("expected error for nil factory")
	}
	if err := r.RegisterVerifier("test", newTestVerifier); err != nil {
		t.Fatalf("RegisterVerifier() error = %v", err)
	}
	if err := r.RegisterVerifier("test", newTestVerifier); err == nil {
		t.Error("expected error for duplicate registration")
	}

	if _, err := r.NewVerifier(ctx, "test", VerifierOptions{}); err == nil {
		t.Error("expected error for empty verifier name")
	}
	if _, err := r.NewVerifier(ctx, "", VerifierOptions{Name: "v"}); err == nil {
		t.Error("expected error for empty type")
	}
	if _, err := r.NewVerifier(ctx, "unknown", VerifierOptions{Name: "v"}); !errors.Is(err, ErrNotRegistered) {
		t.Errorf("expected ErrNotRegistered, got %v", err)
	}
	if _, err := r.NewVerifier(ctx, "test", VerifierOptions{Name: "v", Parameters: json.RawMessage(`{`)}); err == nil {
		t.Error("expected error for invalid parameters")
	}

	if err := r.RegisterVerifier("returns-nil", func(context.Context, VerifierOptions) (ratify.Verifier, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.NewVerifier(ctx, "returns-nil", VerifierOptions{Name: "v"}); err == nil {
		t.Error("expected error for nil verifier")
	}

	var zero Registry
	if err := zero.RegisterVerifier("test", newTestVerifier); err == nil {
		t.Error("expected error for uninitialized registry")
	}
}

func TestRegistry_StorePolicyEnforcerKeyProvider(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	var gotStoreParams, gotPolicyParams, gotKeyParams string
	if err := r.RegisterStore("s", func(_ context.Context, opts StoreOptions) (ratify.Store, error) {
		gotStoreParams = string(opts.Parameters)
		return &testStore{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterPolicyEnforcer("p", func(_ context.Context, opts PolicyEnforcerOptions) (ratify.PolicyEnforcer, error) {
		gotPolicyParams = string(opts.Parameters)
		return &testPolicyEnforcer{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterKeyProvider("k", func(_ context.Context, opts KeyProviderOptions) (KeyProvider, error) {
		gotKeyParams = string(opts.Parameters)
		return &testKeyProvider{}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := r.NewStore(ctx, "s", StoreOptions{Parameters: json.RawMessage(`{"s":1}`)}); err != nil || gotStoreParams != `{"s":1}` {
		t.Errorf("NewStore() err = %v, params = %s", err, gotStoreParams)
	}
	if _, err := r.NewPolicyEnforcer(ctx, "p", PolicyEnforcerOptions{Parameters: json.RawMessage(`{"p":1}`)}); err != nil || gotPolicyParams != `{"p":1}` {
		t.Errorf("NewPolicyEnforcer() err = %v, params = %s", err, gotPolicyParams)
	}
	if _, err := r.NewKeyProvider(ctx, "k", KeyProviderOptions{Parameters: json.RawMessage(`{"k":1}`)}); err != nil || gotKeyParams != `{"k":1}` {
		t.Errorf("NewKeyProvider() err = %v, params = %s", err, gotKeyParams)
	}

	for name, fn := range map[string]func() error{
		"store":           func() error { _, err := r.NewStore(ctx, "x", StoreOptions{}); return err },
		"policy enforcer": func() error { _, err := r.NewPolicyEnforcer(ctx, "x", PolicyEnforcerOptions{}); return err },
		"key provider":    func() error { _, err := r.NewKeyProvider(ctx, "x", KeyProviderOptions{}); return err },
	} {
		if err := fn(); !errors.Is(err, ErrNotRegistered) {
			t.Errorf("%s: expected ErrNotRegistered, got %v", name, err)
		}
	}

	if got := r.StoreTypes(); !reflect.DeepEqual(got, []string{"s"}) {
		t.Errorf("StoreTypes() = %v", got)
	}
	if got := r.PolicyEnforcerTypes(); !reflect.DeepEqual(got, []string{"p"}) {
		t.Errorf("PolicyEnforcerTypes() = %v", got)
	}
	if got := r.KeyProviderTypes(); !reflect.DeepEqual(got, []string{"k"}) {
		t.Errorf("KeyProviderTypes() = %v", got)
	}
}

func TestRegistry_FactoryErrorIsWrapped(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	boom := errors.New("boom")
	_ = r.RegisterVerifier("v", func(context.Context, VerifierOptions) (ratify.Verifier, error) { return nil, boom })
	_ = r.RegisterStore("s", func(context.Context, StoreOptions) (ratify.Store, error) { return nil, boom })
	_ = r.RegisterPolicyEnforcer("p", func(context.Context, PolicyEnforcerOptions) (ratify.PolicyEnforcer, error) { return nil, boom })
	_ = r.RegisterKeyProvider("k", func(context.Context, KeyProviderOptions) (KeyProvider, error) { return nil, boom })

	if _, err := r.NewVerifier(ctx, "v", VerifierOptions{Name: "n"}); !errors.Is(err, boom) {
		t.Errorf("verifier: %v", err)
	}
	if _, err := r.NewStore(ctx, "s", StoreOptions{}); !errors.Is(err, boom) {
		t.Errorf("store: %v", err)
	}
	if _, err := r.NewPolicyEnforcer(ctx, "p", PolicyEnforcerOptions{}); !errors.Is(err, boom) {
		t.Errorf("policy enforcer: %v", err)
	}
	if _, err := r.NewKeyProvider(ctx, "k", KeyProviderOptions{}); !errors.Is(err, boom) {
		t.Errorf("key provider: %v", err)
	}
}

func TestDefaultRegistry(t *testing.T) {
	saved := Default
	t.Cleanup(func() { Default = saved })
	Default = NewRegistry()

	RegisterVerifier("v", newTestVerifier)
	RegisterStore("s", func(context.Context, StoreOptions) (ratify.Store, error) { return &testStore{}, nil })
	RegisterPolicyEnforcer("p", func(context.Context, PolicyEnforcerOptions) (ratify.PolicyEnforcer, error) {
		return &testPolicyEnforcer{}, nil
	})
	RegisterKeyProvider("k", func(context.Context, KeyProviderOptions) (KeyProvider, error) { return &testKeyProvider{}, nil })
	if got := Default.VerifierTypes(); !reflect.DeepEqual(got, []string{"v"}) {
		t.Errorf("VerifierTypes() = %v", got)
	}

	defer func() {
		if recover() == nil {
			t.Error("expected panic on duplicate registration")
		}
	}()
	RegisterVerifier("v", newTestVerifier)
}

func TestRegistry_Concurrent(t *testing.T) {
	r := NewRegistry()
	_ = r.RegisterVerifier("v", newTestVerifier)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = r.NewVerifier(context.Background(), "v", VerifierOptions{Name: "n"})
		}()
		go func(i int) {
			defer wg.Done()
			_ = r.RegisterKeyProvider(string(rune('a'+i)), func(context.Context, KeyProviderOptions) (KeyProvider, error) {
				return &testKeyProvider{}, nil
			})
		}(i)
	}
	wg.Wait()
}

func TestDecodeParameters(t *testing.T) {
	type params struct {
		A string `json:"a"`
	}
	for _, raw := range []string{"", "  ", "null"} {
		p := params{A: "keep"}
		if err := DecodeParameters(json.RawMessage(raw), &p); err != nil || p.A != "keep" {
			t.Errorf("DecodeParameters(%q) = %v, %+v", raw, err, p)
		}
	}
	var p params
	if err := DecodeParameters(json.RawMessage(`{"a":"b"}`), &p); err != nil || p.A != "b" {
		t.Errorf("DecodeParameters() = %v, %+v", err, p)
	}
	if err := DecodeParameters(json.RawMessage(`{`), &p); err == nil {
		t.Error("expected error for invalid JSON")
	}
}
