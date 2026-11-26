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

// Package plugin is the compile-time plugin framework of Ratify.
//
// A plugin is a Go package that implements one of the Ratify interfaces
// ([ratify.Verifier], [ratify.Store], [ratify.PolicyEnforcer] or
// [KeyProvider]) and registers a factory for it under a type name. An
// application, such as the Ratify Gatekeeper provider, reads its configuration
// as a list of {type, name, parameters} entries and asks a [Registry] to create
// the instances. The application never parses the parameters itself; each
// factory owns the schema of its parameters.
//
// Plugins usually register themselves to the [Default] registry from an init
// function, so that an application only needs a blank import to enable them,
// in the same way as database/sql drivers:
//
//	import _ "github.com/notaryproject/ratify-verifier-go/notation/register"
//
// Libraries that embed Ratify and construct verifiers directly do not need
// this package at all.
package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/notaryproject/ratify-go"
)

// ErrNotRegistered is returned when no factory is registered for a type.
var ErrNotRegistered = errors.New("plugin type is not registered")

// VerifierOptions are the options passed to a [VerifierFactory].
type VerifierOptions struct {
	// Name is the unique name of the verifier instance. Required.
	Name string

	// Parameters is the raw, plugin specific configuration of the verifier.
	// The factory is responsible for decoding and validating it. Optional.
	Parameters json.RawMessage

	// Scopes are the default scopes inherited from the enclosing executor
	// configuration. A factory may use them when its parameters do not specify
	// scopes explicitly. Optional.
	Scopes []string

	// Registry is the registry that is creating the verifier. Factories use it
	// to resolve nested plugins, such as key providers, so that they honor the
	// same set of registered plugins as the caller. It is never nil when the
	// factory is invoked through [Registry.NewVerifier].
	Registry *Registry
}

// StoreOptions are the options passed to a [StoreFactory].
type StoreOptions struct {
	// Parameters is the raw, plugin specific configuration of the store.
	// Optional.
	Parameters json.RawMessage

	// Registry is the registry that is creating the store. It is never nil
	// when the factory is invoked through [Registry.NewStore].
	Registry *Registry
}

// PolicyEnforcerOptions are the options passed to a [PolicyEnforcerFactory].
type PolicyEnforcerOptions struct {
	// Parameters is the raw, plugin specific configuration of the policy
	// enforcer. Optional.
	Parameters json.RawMessage

	// Registry is the registry that is creating the policy enforcer. It is
	// never nil when the factory is invoked through
	// [Registry.NewPolicyEnforcer].
	Registry *Registry
}

// KeyProviderOptions are the options passed to a [KeyProviderFactory].
type KeyProviderOptions struct {
	// Parameters is the raw, plugin specific configuration of the key
	// provider. Optional.
	Parameters json.RawMessage

	// Registry is the registry that is creating the key provider. It is never
	// nil when the factory is invoked through [Registry.NewKeyProvider].
	Registry *Registry
}

// VerifierFactory creates a [ratify.Verifier] from its options.
type VerifierFactory func(ctx context.Context, opts VerifierOptions) (ratify.Verifier, error)

// StoreFactory creates a [ratify.Store] from its options.
type StoreFactory func(ctx context.Context, opts StoreOptions) (ratify.Store, error)

// PolicyEnforcerFactory creates a [ratify.PolicyEnforcer] from its options.
type PolicyEnforcerFactory func(ctx context.Context, opts PolicyEnforcerOptions) (ratify.PolicyEnforcer, error)

// KeyProviderFactory creates a [KeyProvider] from its options.
type KeyProviderFactory func(ctx context.Context, opts KeyProviderOptions) (KeyProvider, error)

// Registry maps plugin type names to factories. The zero value is not usable;
// use [NewRegistry]. A Registry is safe for concurrent use.
type Registry struct {
	mu              sync.RWMutex
	verifiers       map[string]VerifierFactory
	stores          map[string]StoreFactory
	policyEnforcers map[string]PolicyEnforcerFactory
	keyProviders    map[string]KeyProviderFactory
}

// NewRegistry creates an empty [Registry].
func NewRegistry() *Registry {
	return &Registry{
		verifiers:       make(map[string]VerifierFactory),
		stores:          make(map[string]StoreFactory),
		policyEnforcers: make(map[string]PolicyEnforcerFactory),
		keyProviders:    make(map[string]KeyProviderFactory),
	}
}

// Default is the registry that plugins register themselves to from their init
// functions and that applications use by default.
var Default = NewRegistry()

// RegisterVerifier registers a verifier factory under the given type.
func (r *Registry) RegisterVerifier(typ string, factory VerifierFactory) error {
	return register(&r.mu, r.verifiers, "verifier", typ, factory)
}

// RegisterStore registers a store factory under the given type.
func (r *Registry) RegisterStore(typ string, factory StoreFactory) error {
	return register(&r.mu, r.stores, "store", typ, factory)
}

// RegisterPolicyEnforcer registers a policy enforcer factory under the given
// type.
func (r *Registry) RegisterPolicyEnforcer(typ string, factory PolicyEnforcerFactory) error {
	return register(&r.mu, r.policyEnforcers, "policy enforcer", typ, factory)
}

// RegisterKeyProvider registers a key provider factory under the given type.
func (r *Registry) RegisterKeyProvider(typ string, factory KeyProviderFactory) error {
	return register(&r.mu, r.keyProviders, "key provider", typ, factory)
}

// NewVerifier creates a verifier of the given type.
func (r *Registry) NewVerifier(ctx context.Context, typ string, opts VerifierOptions) (ratify.Verifier, error) {
	if opts.Name == "" {
		return nil, errors.New("verifier name is required")
	}
	factory, err := lookup(&r.mu, r.verifiers, "verifier", typ)
	if err != nil {
		return nil, err
	}
	opts.Registry = r
	v, err := factory(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create verifier %q of type %q: %w", opts.Name, typ, err)
	}
	if v == nil {
		return nil, fmt.Errorf("verifier factory of type %q returned nil", typ)
	}
	return v, nil
}

// NewStore creates a store of the given type.
func (r *Registry) NewStore(ctx context.Context, typ string, opts StoreOptions) (ratify.Store, error) {
	factory, err := lookup(&r.mu, r.stores, "store", typ)
	if err != nil {
		return nil, err
	}
	opts.Registry = r
	s, err := factory(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create store of type %q: %w", typ, err)
	}
	if s == nil {
		return nil, fmt.Errorf("store factory of type %q returned nil", typ)
	}
	return s, nil
}

// NewPolicyEnforcer creates a policy enforcer of the given type.
func (r *Registry) NewPolicyEnforcer(ctx context.Context, typ string, opts PolicyEnforcerOptions) (ratify.PolicyEnforcer, error) {
	factory, err := lookup(&r.mu, r.policyEnforcers, "policy enforcer", typ)
	if err != nil {
		return nil, err
	}
	opts.Registry = r
	p, err := factory(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create policy enforcer of type %q: %w", typ, err)
	}
	if p == nil {
		return nil, fmt.Errorf("policy enforcer factory of type %q returned nil", typ)
	}
	return p, nil
}

// NewKeyProvider creates a key provider of the given type.
func (r *Registry) NewKeyProvider(ctx context.Context, typ string, opts KeyProviderOptions) (KeyProvider, error) {
	factory, err := lookup(&r.mu, r.keyProviders, "key provider", typ)
	if err != nil {
		return nil, err
	}
	opts.Registry = r
	p, err := factory(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create key provider of type %q: %w", typ, err)
	}
	if p == nil {
		return nil, fmt.Errorf("key provider factory of type %q returned nil", typ)
	}
	return p, nil
}

// VerifierTypes returns the sorted list of registered verifier types.
func (r *Registry) VerifierTypes() []string { return keys(&r.mu, r.verifiers) }

// StoreTypes returns the sorted list of registered store types.
func (r *Registry) StoreTypes() []string { return keys(&r.mu, r.stores) }

// PolicyEnforcerTypes returns the sorted list of registered policy enforcer
// types.
func (r *Registry) PolicyEnforcerTypes() []string { return keys(&r.mu, r.policyEnforcers) }

// KeyProviderTypes returns the sorted list of registered key provider types.
func (r *Registry) KeyProviderTypes() []string { return keys(&r.mu, r.keyProviders) }

// RegisterVerifier registers a verifier factory to the [Default] registry.
// It panics if the type is empty, the factory is nil or the type is already
// registered, so that misconfigured builds fail fast at start-up. It is
// intended to be called from init functions.
func RegisterVerifier(typ string, factory VerifierFactory) {
	must(Default.RegisterVerifier(typ, factory))
}

// RegisterStore registers a store factory to the [Default] registry. It panics
// on error. See [RegisterVerifier].
func RegisterStore(typ string, factory StoreFactory) {
	must(Default.RegisterStore(typ, factory))
}

// RegisterPolicyEnforcer registers a policy enforcer factory to the [Default]
// registry. It panics on error. See [RegisterVerifier].
func RegisterPolicyEnforcer(typ string, factory PolicyEnforcerFactory) {
	must(Default.RegisterPolicyEnforcer(typ, factory))
}

// RegisterKeyProvider registers a key provider factory to the [Default]
// registry. It panics on error. See [RegisterVerifier].
func RegisterKeyProvider(typ string, factory KeyProviderFactory) {
	must(Default.RegisterKeyProvider(typ, factory))
}

func register[F any](mu *sync.RWMutex, m map[string]F, kind, typ string, factory F) error {
	if typ == "" {
		return fmt.Errorf("%s type cannot be empty", kind)
	}
	if any(factory) == nil || isNilFunc(factory) {
		return fmt.Errorf("%s factory of type %q cannot be nil", kind, typ)
	}
	mu.Lock()
	defer mu.Unlock()
	if m == nil {
		return errors.New("registry is not initialized, use plugin.NewRegistry")
	}
	if _, ok := m[typ]; ok {
		return fmt.Errorf("%s factory of type %q is already registered", kind, typ)
	}
	m[typ] = factory
	return nil
}

func lookup[F any](mu *sync.RWMutex, m map[string]F, kind, typ string) (F, error) {
	var zero F
	if typ == "" {
		return zero, fmt.Errorf("%s type is required", kind)
	}
	mu.RLock()
	defer mu.RUnlock()
	factory, ok := m[typ]
	if !ok {
		return zero, fmt.Errorf("%w: %s type %q", ErrNotRegistered, kind, typ)
	}
	return factory, nil
}

func keys[F any](mu *sync.RWMutex, m map[string]F) []string {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

// isNilFunc reports whether a factory function value is nil.
func isNilFunc(f any) bool {
	switch fn := f.(type) {
	case VerifierFactory:
		return fn == nil
	case StoreFactory:
		return fn == nil
	case PolicyEnforcerFactory:
		return fn == nil
	case KeyProviderFactory:
		return fn == nil
	}
	return false
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
