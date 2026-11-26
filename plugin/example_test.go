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

package plugin_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/notaryproject/ratify-go"
	"github.com/notaryproject/ratify-go/plugin"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// allowListVerifier is a toy verifier that only accepts artifacts whose
// artifact type is in an allow list.
type allowListVerifier struct {
	name    string
	allowed map[string]bool
}

func (v *allowListVerifier) Name() string { return v.name }
func (v *allowListVerifier) Type() string { return "allowlist" }
func (v *allowListVerifier) Verifiable(desc ocispec.Descriptor) bool {
	return v.allowed[desc.ArtifactType]
}
func (v *allowListVerifier) Verify(_ context.Context, _ *ratify.VerifyOptions) (*ratify.VerificationResult, error) {
	return &ratify.VerificationResult{Verifier: v, Description: "allowed"}, nil
}

// A third-party plugin registers its factory in an init function. The
// factory owns the schema of its parameters.
func init() {
	plugin.RegisterVerifier("allowlist", func(_ context.Context, opts plugin.VerifierOptions) (ratify.Verifier, error) {
		var params struct {
			ArtifactTypes []string `json:"artifactTypes"`
		}
		if err := plugin.DecodeParameters(opts.Parameters, &params); err != nil {
			return nil, err
		}
		v := &allowListVerifier{name: opts.Name, allowed: map[string]bool{}}
		for _, t := range params.ArtifactTypes {
			v.allowed[t] = true
		}
		return v, nil
	})
}

// An application creates plugin instances from configuration without knowing
// the concrete plugin types.
func Example() {
	config := `{"name": "sbom-check", "type": "allowlist", "parameters": {"artifactTypes": ["application/spdx+json"]}}`
	var entry struct {
		Name       string          `json:"name"`
		Type       string          `json:"type"`
		Parameters json.RawMessage `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(config), &entry); err != nil {
		panic(err)
	}

	v, err := plugin.Default.NewVerifier(context.Background(), entry.Type, plugin.VerifierOptions{
		Name:       entry.Name,
		Parameters: entry.Parameters,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(v.Name(), v.Verifiable(ocispec.Descriptor{ArtifactType: "application/spdx+json"}))
	// Output: sbom-check true
}
