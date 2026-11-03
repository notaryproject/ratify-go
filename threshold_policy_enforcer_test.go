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
	"sync"
	"testing"
)

func TestThresholdEvaluator_Concurrent(t *testing.T) {
	ctx := context.Background()
	enforcer, err := NewThresholdPolicyEnforcer(&ThresholdPolicyRule{
		Rules: []*ThresholdPolicyRule{
			{Rules: []*ThresholdPolicyRule{
				{Verifier: notationVerifier1, Rules: []*ThresholdPolicyRule{
					{Verifier: sbomVerifier1},
				}},
			}},
			{Verifier: notationVerifier2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := enforcer.Evaluator(ctx, imageDigest)
	if err != nil {
		t.Fatal(err)
	}

	const referrerCount = 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range referrerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			artifactDigest := fmt.Sprintf("notation-%d", i)
			if _, err := evaluator.Pruned(ctx, imageDigest, artifactDigest, notationVerifier1); err != nil {
				t.Error(err)
			}
			if err := evaluator.AddResult(ctx, imageDigest, artifactDigest, &VerificationResult{
				Verifier: &mockVerifier{name: notationVerifier1},
			}); err != nil {
				t.Error(err)
				return
			}
			if err := evaluator.AddResult(ctx, artifactDigest, fmt.Sprintf("sbom-%d", i), &VerificationResult{
				Verifier: &mockVerifier{name: sbomVerifier1},
			}); err != nil {
				t.Error(err)
				return
			}
			if err := evaluator.Commit(ctx, artifactDigest); err != nil {
				t.Error(err)
			}
			if allowed, err := evaluator.Evaluate(ctx); err != nil || allowed {
				t.Errorf("Evaluate() = %v, %v; want false, nil", allowed, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	state := evaluator.(*thresholdEvaluator)
	if got := len(state.subjectIndex); got != 1+2*referrerCount {
		t.Errorf("subject index has %d entries, want %d", got, 1+2*referrerCount)
	}
	if got := len(state.verifierIndex); got != 2*referrerCount {
		t.Errorf("verifier index has %d entries, want %d", got, 2*referrerCount)
	}
	if err := evaluator.AddResult(ctx, imageDigest, notationDigest2, &VerificationResult{
		Verifier: &mockVerifier{name: notationVerifier2},
	}); err != nil {
		t.Fatal(err)
	}
	if err := evaluator.Commit(ctx, imageDigest); err != nil {
		t.Fatal(err)
	}
	if allowed, err := evaluator.Evaluate(ctx); err != nil || !allowed {
		t.Errorf("Evaluate() = %v, %v; want true, nil", allowed, err)
	}
}

const (
	imageDigest       = "image-digest"
	notationDigest1   = "notation-digest-1"
	notationDigest2   = "notation-digest-2"
	notationDigest3   = "notation-digest-3"
	notationDigest4   = "notation-digest-4"
	sbomDigest1       = "sbom-digest-1"
	sbomDigest2       = "sbom-digest-2"
	sbomDigest3       = "sbom-digest-3"
	sbomVerifier1     = "sbom-verifier-1"
	sbomVerifier2     = "sbom-verifier-2"
	sbomVerifier3     = "sbom-verifier-3"
	sbomVerifier4     = "sbom-verifier-4"
	sbomVerifier5     = "sbom-verifier-5"
	notationVerifier1 = "notation-verifier-1"
	notationVerifier2 = "notation-verifier-2"
	cosignVerifier1   = "cosign-verifier-1"
)

func TestNewThresholdPolicyEnforcer(t *testing.T) {
	tests := []struct {
		name        string
		policy      *ThresholdPolicyRule
		expectError bool
	}{
		{
			name:        "nil policy",
			policy:      nil,
			expectError: true,
		},
		{
			name: "valid policy with nested rules",
			policy: &ThresholdPolicyRule{
				Threshold: 1,
				Rules: []*ThresholdPolicyRule{
					{Verifier: "verifier1"},
					{Verifier: "verifier2"},
				},
			},
			expectError: false,
		},
		{
			name: "invalid policy with verifier name at root",
			policy: &ThresholdPolicyRule{
				Verifier: "rootVerifier",
			},
			expectError: true,
		},
		{
			name: "invalid policy with negative threshold",
			policy: &ThresholdPolicyRule{
				Threshold: -1,
				Rules: []*ThresholdPolicyRule{
					{Verifier: "verifier1"},
				},
			},
			expectError: true,
		},
		{
			name: "invalid policy with threshold greater than rules count",
			policy: &ThresholdPolicyRule{
				Threshold: 3,
				Rules: []*ThresholdPolicyRule{
					{Verifier: "verifier1"},
					{Verifier: "verifier2"},
				},
			},
			expectError: true,
		},
		{
			name: "invalid policy with no verifier and no nested rules",
			policy: &ThresholdPolicyRule{
				Threshold: 0,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enforcer, err := NewThresholdPolicyEnforcer(tt.policy)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && enforcer == nil {
				t.Errorf("expected enforcer to be non-nil")
			}
		})
	}
}
func TestThresholdPolicyEnforcer_Evaluator(t *testing.T) {
	validPolicy := &ThresholdPolicyRule{
		Threshold: 1,
		Rules: []*ThresholdPolicyRule{
			{Verifier: "verifier1"},
			{Verifier: "verifier2"},
		},
	}

	enforcer, err := NewThresholdPolicyEnforcer(validPolicy)
	if err != nil {
		t.Fatalf("unexpected error creating enforcer: %v", err)
	}

	tests := []struct {
		name           string
		policyEnforcer *ThresholdPolicyEnforcer
		subjectDigest  string
		expectError    bool
	}{
		{
			name:           "valid evaluator creation",
			policyEnforcer: enforcer,
			subjectDigest:  "digest1",
			expectError:    false,
		},
		{
			name:           "nil policy enforcer",
			policyEnforcer: &ThresholdPolicyEnforcer{policy: nil},
			subjectDigest:  "digest2",
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := tt.policyEnforcer.Evaluator(context.Background(), tt.subjectDigest)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && evaluator == nil {
				t.Errorf("expected evaluator to be non-nil")
			}
		})
	}
}

func TestEvaluation_SingleSignature(t *testing.T) {
	// The testing policy can be represented in yaml as:
	// - rules:
	//    - verifier: notation-verifier-1
	policy := &ThresholdPolicyRule{
		Rules: []*ThresholdPolicyRule{
			{
				Verifier: notationVerifier1,
			},
		},
	}

	// The image that will be tested against the policy is in the following
	// structure:
	// image-digest
	// └── notation-digest-1
	enforcer, err := NewThresholdPolicyEnforcer(policy)
	if err != nil {
		t.Fatalf("unexpected error creating enforcer: %v", err)
	}
	ctx := context.Background()
	evaluator, err := enforcer.Evaluator(ctx, imageDigest)
	if err != nil {
		t.Fatalf("unexpected error creating evaluator: %v", err)
	}

	state, err := evaluator.Pruned(ctx, imageDigest, notationDigest1, notationVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}

	// Add a failed verification result.
	// This should not affect the evaluation.
	result := &VerificationResult{
		Verifier: &mockVerifier{name: notationVerifier1},
		Err:      errors.New("verification error"),
	}
	if err = evaluator.AddResult(ctx, imageDigest, notationDigest1, result); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}
	if err = evaluator.Commit(ctx, notationDigest1); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}

	// Add a successful verification result.
	// This should affect the evaluation.
	result = &VerificationResult{
		Verifier: &mockVerifier{name: notationVerifier1},
	}
	if err = evaluator.AddResult(ctx, imageDigest, notationDigest1, result); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}
	if err = evaluator.Commit(ctx, notationDigest1); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}
	if err = evaluator.Commit(ctx, imageDigest); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}

	// Evaluate the results.
	decision, err := evaluator.Evaluate(ctx)
	if err != nil {
		t.Fatalf("unexpected error evaluating: %v", err)
	}
	if decision != true {
		t.Fatalf("expected decision true, got %v", decision)
	}
}

func TestEvaluation(t *testing.T) {
	// The testing policy can be represented in yaml as:
	// - threshold: 2
	//   rules:
	//     - verifier: notation-verifier-1
	//     - verifier: sbom-verifier-1
	//       rules:
	//         - verifier: notation-verifier-1
	//         - threshold: 1
	//		     rules:
	//         	   - verifier: sbom-verifier-3
	//           	 rules:
	//             	   - verifier: notation-verifier-1
	//             - verifier: sbom-verifier-4
	//               rules:
	//                 - verifier: notation-verifier-1
	//          - threshold: 1
	//		     rules:
	//         	   - verifier: sbom-verifier-3
	//           	 rules:
	//             	   - verifier: notation-verifier-1
	//             - verifier: sbom-verifier-5
	//               rules:
	//                 - verifier: notation-verifier-1
	//     - verifier: sbom-verifier-2
	//       rules:
	//         - verifier: cosign-verifier-1
	//         - verifier: sbom-verifier-3
	//           rules:
	//             - verifier: cosign-verifier-1
	policy := &ThresholdPolicyRule{
		Threshold: 2,
		Rules: []*ThresholdPolicyRule{
			{
				Verifier: notationVerifier1,
			},
			{
				Verifier: sbomVerifier1,
				Rules: []*ThresholdPolicyRule{
					{
						Verifier: notationVerifier1,
					},
					{
						Threshold: 1,
						Rules: []*ThresholdPolicyRule{
							{
								Verifier: sbomVerifier3,
								Rules: []*ThresholdPolicyRule{
									{
										Verifier: notationVerifier1,
									},
								},
							},
							{
								Verifier: sbomVerifier4,
								Rules: []*ThresholdPolicyRule{
									{
										Verifier: notationVerifier1,
									},
								},
							},
						},
					},
					{
						Threshold: 1,
						Rules: []*ThresholdPolicyRule{
							{
								Verifier: sbomVerifier3,
								Rules: []*ThresholdPolicyRule{
									{
										Verifier: notationVerifier1,
									},
								},
							},
							{
								Verifier: sbomVerifier5,
								Rules: []*ThresholdPolicyRule{
									{
										Verifier: notationVerifier1,
									},
								},
							},
						},
					},
				},
			},
			{
				Verifier: sbomVerifier2,
				Rules: []*ThresholdPolicyRule{
					{
						Verifier: cosignVerifier1,
					},
					{
						Verifier: sbomVerifier1,
						Rules: []*ThresholdPolicyRule{
							{
								Verifier: cosignVerifier1,
							},
						},
					},
				},
			},
		},
	}

	// The image that will be tested against the policy is in the following
	// structure:
	// image-digest
	// ├── notation-digest-1
	// └── sbom-digest-1
	//     ├── notation-digest-2
	//     └── sbom-digest-2
	//         └── notation-digest-3
	enforcer, err := NewThresholdPolicyEnforcer(policy)
	if err != nil {
		t.Fatalf("unexpected error creating enforcer: %v", err)
	}
	ctx := context.Background()
	evaluator, err := enforcer.Evaluator(ctx, imageDigest)
	if err != nil {
		t.Fatalf("unexpected error creating evaluator: %v", err)
	}

	if state, err := evaluator.Pruned(ctx, sbomDigest1, notationDigest1, notationVerifier1); err != nil || state != PrunedStateSubjectPruned {
		t.Fatalf("expected no error and subject pruned state, got error: %v, state: %v", err, state)
	}

	state, err := evaluator.Pruned(ctx, imageDigest, notationDigest1, notationVerifier2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateVerifierPruned {
		t.Fatalf("expected verifier pruned state, got %v", state)
	}

	state, err = evaluator.Pruned(ctx, imageDigest, notationDigest1, notationVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}

	if err = evaluator.AddResult(ctx, imageDigest, notationDigest1, &VerificationResult{Err: errors.New("verification error")}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}
	if err = evaluator.AddResult(ctx, imageDigest, notationDigest1, &VerificationResult{Verifier: &mockVerifier{name: notationVerifier1}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}
	decision, err := evaluator.Evaluate(ctx)
	if err != nil {
		t.Fatalf("unexpected error evaluating: %v", err)
	}
	if decision != false {
		t.Fatalf("expected decision false, got %v", decision)
	}

	state, err = evaluator.Pruned(ctx, imageDigest, sbomDigest1, sbomVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}
	if err = evaluator.AddResult(ctx, imageDigest, sbomDigest1, &VerificationResult{Verifier: &mockVerifier{name: sbomVerifier1}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}
	state, err = evaluator.Pruned(ctx, imageDigest, sbomDigest1, sbomVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateVerifierPruned {
		t.Fatalf("expected verifier pruned state, got %v", state)
	}

	state, err = evaluator.Pruned(ctx, imageDigest, sbomDigest1, sbomVerifier2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}
	if err = evaluator.AddResult(ctx, imageDigest, sbomDigest1, &VerificationResult{Verifier: &mockVerifier{name: sbomVerifier2}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}

	if err = evaluator.Commit(ctx, imageDigest); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}

	state, err = evaluator.Pruned(ctx, sbomDigest1, notationDigest2, notationVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}
	if err = evaluator.AddResult(ctx, sbomDigest1, notationDigest2, &VerificationResult{Verifier: &mockVerifier{name: notationVerifier1}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}

	state, err = evaluator.Pruned(ctx, sbomDigest1, sbomDigest2, sbomVerifier3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}
	if err = evaluator.AddResult(ctx, sbomDigest1, sbomDigest2, &VerificationResult{Verifier: &mockVerifier{name: sbomVerifier3}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}

	if err = evaluator.Commit(ctx, sbomDigest1); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}

	state, err = evaluator.Pruned(ctx, sbomDigest2, notationDigest3, notationVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateNone {
		t.Fatalf("expected none pruned state, got %v", state)
	}
	if err = evaluator.AddResult(ctx, sbomDigest2, notationDigest3, &VerificationResult{Verifier: &mockVerifier{name: notationVerifier1}}); err != nil {
		t.Fatalf("unexpected error adding result: %v", err)
	}

	state, err = evaluator.Pruned(ctx, sbomDigest2, notationDigest4, notationVerifier1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != PrunedStateSubjectPruned {
		t.Fatalf("expected subject pruned state, got %v", state)
	}

	if err = evaluator.Commit(ctx, sbomDigest2); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}

	decision, err = evaluator.Evaluate(ctx)
	if err != nil {
		t.Fatalf("unexpected error evaluating: %v", err)
	}
	if decision != true {
		t.Fatalf("expected decision true, got %v", decision)
	}
}
