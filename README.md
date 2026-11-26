# Ratify Go Library

> [!WARNING]
> This project is currently under initial development. APIs may and will be changed incompatibly from one commit to another.

[![Build Status](https://github.com/notaryproject/ratify-go/actions/workflows/build.yml/badge.svg?event=push&branch=main)](https://github.com/notaryproject/ratify-go/actions/workflows/build.yml?query=workflow%3Abuild+event%3Apush+branch%3Amain)
[![Codecov](https://codecov.io/gh/ratify-project/ratify-go/branch/main/graph/badge.svg)](https://codecov.io/gh/ratify-project/ratify-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/notaryproject/ratify-go.svg)](https://pkg.go.dev/github.com/notaryproject/ratify-go@main)

`ratify-go` provides core functionality for the ratify library.

## Packages

| Package | Description |
|---|---|
| [`ratify`](https://pkg.go.dev/github.com/notaryproject/ratify-go) | Core interfaces (`Verifier`, `Store`, `PolicyEnforcer`) and the `Executor`. |
| [`plugin`](https://pkg.go.dev/github.com/notaryproject/ratify-go/plugin) | Compile-time plugin framework: a registry that maps plugin type names to factories, plus the shared `KeyProvider` interface. |
| [`imageref`](https://pkg.go.dev/github.com/notaryproject/ratify-go/imageref) | Normalization of image references and scopes, following container runtime rules. |

### Writing a plugin

A plugin implements one of the core interfaces and registers a factory for it,
usually from an `init` function of a dedicated `register` package:

```go
package register

import "github.com/notaryproject/ratify-go/plugin"

func init() {
	plugin.RegisterVerifier("my-verifier", myverifier.Factory)
}
```

An application enables the plugin with a blank import and creates instances
from configuration through `plugin.Default`. The application passes the plugin
parameters through as raw JSON; each factory owns its parameter schema.

## Versioning

The `ratify-go` library follows [Semantic Versioning](https://semver.org/), where breaking changes are reserved for MAJOR releases, and MINOR and PATCH releases must be 100% backwards compatible.

## Documentation

- [Ratify Concepts](https://ratify.dev/docs/category/concepts): Documentation of framework, executor, store, and verifier concepts.
- [Go Reference](https://pkg.go.dev/github.com/notaryproject/ratify-go@main): Library documentation.

## Contributing
Thanks for your interest in contributing! There are many ways to contribute to this project. 
Get started here: [CONTRIBUTING.md](./CONTRIBUTING.md)

## Code of Conduct

This project has adopted the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).
