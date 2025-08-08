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
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	testRepo         = "test-registry/test-repo"
	testDigest1      = "sha256:cd0abf4135161b8aeb079b64b8215e433088d21463204771d070aadc52678aa0"
	testDigest2      = "sha256:e05b6fbf2432faf87115041d172aa1f587cff725b94c61d927f67c21e1e2d5b9"
	testDigest3      = "sha256:5ca41da4799a48a58ec307678155c52a37caad54492a96854b14d8c856a8c5d8"
	testDigest4      = "sha256:97fd9660fd193c8671ffa322453bf21e46ab8ab6543f82b065caa7f014155bc4"
	testDigest5      = "sha256:87f06eb9e99f17e1a57346c388d60e636a725f7d9bce33fb90e54156d36297e9"
	testImage        = testRepo + ":v1"
	testArtifact1    = testRepo + "@" + testDigest1
	testArtifact2    = testRepo + "@" + testDigest2
	testArtifact3    = testRepo + "@" + testDigest3
	testArtifact4    = testRepo + "@" + testDigest4
	validMessage1    = "valid signature 1"
	validMessage2    = "valid signature 2"
	validMessage3    = "valid signature 3"
	validMessage4    = "valid signature 4"
	validMessage5    = "valid signature 5"
	artifactTypeSig  = "application/vnd.dev.ratify.signature.v1+json"
	artifactTypeSBoM = "application/vnd.dev.ratify.sbom.v1+json"
)

// mockVerifier is a mock implementation of Verifier.
type mockVerifier struct {
	name            string
	verifiable      bool
	verifiableTypes []string
	verifyResult    map[string]*VerificationResult
}

func (m *mockVerifier) Name() string {
	if m.name != "" {
		return m.name
	}
	return "mock-verifier-name"
}

func (m *mockVerifier) Type() string {
	return "mock-verifier-type"
}

func (m *mockVerifier) Verifiable(desc ocispec.Descriptor) bool { // MODIFIED
	if len(m.verifiableTypes) > 0 {
		for _, validType := range m.verifiableTypes {
			if desc.ArtifactType == validType {
				return true
			}
		}
		return false
	}
	return m.verifiable // Fallback to existing behavior for other tests
}

func (m *mockVerifier) Verify(ctx context.Context, opts *VerifyOptions) (*VerificationResult, error) {
	if opts.SubjectDescriptor.Digest == "" {
		return nil, errors.New("subject digest not set")
	}
	if m.verifyResult == nil {
		return &VerificationResult{}, errors.New("verify result not initialized")
	}
	if result, ok := m.verifyResult[opts.ArtifactDescriptor.Digest.String()]; ok {
		return result, nil
	}
	return nil, errors.New("verification result not found for digest: " + opts.ArtifactDescriptor.Digest.String())
}

// mockStore is a mock implementation of Store.
type mockStore struct {
	tagToDesc         map[string]ocispec.Descriptor
	digestToReferrers map[string][]ocispec.Descriptor
}

func (m *mockStore) Name() string {
	return "mock-store-name"
}

func (m *mockStore) ListReferrers(ctx context.Context, ref string, artifactTypes []string, fn func(referrers []ocispec.Descriptor) error) error {
	if m.digestToReferrers == nil {
		return errors.New("referrers not initialized")
	}
	referrers := m.digestToReferrers[ref]
	return fn(referrers)
}

func (m *mockStore) FetchBlob(ctx context.Context, repo string, desc ocispec.Descriptor) ([]byte, error) {
	return nil, nil
}

func (m *mockStore) FetchManifest(ctx context.Context, repo string, desc ocispec.Descriptor) ([]byte, error) {
	return nil, nil
}

func (m *mockStore) Resolve(ctx context.Context, ref string) (ocispec.Descriptor, error) {
	if m.tagToDesc == nil {
		return ocispec.Descriptor{}, errors.New("artifact to descriptor not initialized")
	}
	if desc, ok := m.tagToDesc[ref]; ok {
		return desc, nil
	}

	return ocispec.Descriptor{}, nil
}

type mockEvaluator struct {
	returnEvaluateErr    bool
	returnPrunedErr      bool
	returnAddResultErr   bool
	returnCommitErr      bool
	verifierPrunedDigest string
	artifactPrunedDigest string
	subjectPrunedDigest  string
}

func (m *mockEvaluator) AddResult(ctx context.Context, subjectDigest, artifactDigest string, report *VerificationResult) error {
	if m.returnAddResultErr {
		return errors.New("error happened adding result")
	}
	return nil
}

func (m *mockEvaluator) Pruned(ctx context.Context, subjectDigest, artifactDigest, verifierName string) (PrunedState, error) {
	if m.returnPrunedErr {
		return PrunedStateNone, errors.New("error happened checking pruned state")
	}
	if m.verifierPrunedDigest != "" && artifactDigest == m.verifierPrunedDigest {
		return PrunedStateVerifierPruned, nil
	}
	if m.artifactPrunedDigest != "" && artifactDigest == m.artifactPrunedDigest {
		return PrunedStateArtifactPruned, nil
	}
	if m.subjectPrunedDigest != "" && subjectDigest == m.subjectPrunedDigest {
		return PrunedStateSubjectPruned, nil
	}
	return PrunedStateNone, nil
}

func (m *mockEvaluator) Commit(ctx context.Context, artifactDigest string) error {
	if m.returnCommitErr {
		return errors.New("error happened committing")
	}
	return nil
}

func (m *mockEvaluator) Evaluate(ctx context.Context) (bool, error) {
	if m.returnEvaluateErr {
		return false, errors.New("error happened evaluating")
	}
	return true, nil
}

// mockPolicyEnforcer is a mock implementation of PolicyEnforcer.
type mockPolicyEnforcer struct {
	evaluator *mockEvaluator
}

func (m *mockPolicyEnforcer) Evaluator(ctx context.Context, subjectDigest string) (Evaluator, error) {
	if m.evaluator == nil {
		return nil, errors.New("error happened creating evaluator")
	}
	return m.evaluator, nil
}

func TestValidateArtifact(t *testing.T) {
	tests := []struct {
		name           string
		opts           ValidateArtifactOptions
		store          Store
		verifiers      []Verifier
		policyEnforcer PolicyEnforcer
		want           *ValidationResult
		wantErr        bool
	}{
		{
			name: "Invalid reference",
			opts: ValidateArtifactOptions{
				Subject:        "testrepo:v1",
				ReferenceTypes: []string{"referenceType"},
			},
			store:          &mockStore{},
			verifiers:      []Verifier{&mockVerifier{}},
			policyEnforcer: &mockPolicyEnforcer{},
			want:           nil,
			wantErr:        true,
		},
		{
			name: "Error happened when resolving reference",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store:          &mockStore{},
			verifiers:      []Verifier{&mockVerifier{}},
			policyEnforcer: &mockPolicyEnforcer{},
			want:           nil,
			wantErr:        true,
		},
		{
			name: "Error happened when listing referrers",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
			},
			verifiers:      []Verifier{&mockVerifier{}},
			policyEnforcer: &mockPolicyEnforcer{},
			want:           nil,
			wantErr:        true,
		},
		{
			name: "No referrers attached to the artifact",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {},
				},
			},
			verifiers: []Verifier{&mockVerifier{}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
			},
			wantErr: false,
		},
		{
			name: "Verifier is unable to verify the artifact",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results:         []*VerificationResult{},
						ArtifactReports: []*ValidationReport{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Error happened when verifying the artifact",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Evaluator failed to prune the artifact",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					returnPrunedErr: true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verifier returned result without error",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage1,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Verifier returned result without error but evaluator failed to add result",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					returnAddResultErr: true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verifier returned result without error but Evaluate failed",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					returnEvaluateErr: true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verifier returned result without error but evaluator failed to commit",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					returnCommitErr: true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Policy enforcer is not set",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: nil,
			want: &ValidationResult{
				Succeeded: false,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage1,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Policy enforcer returns error",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage1,
					},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{},
			want:           nil,
			wantErr:        true,
		},
		{
			name: "3-layer nested artifacts are verified",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
						{
							Digest: testDigest3,
						},
					},
					testArtifact2: {
						{
							Digest: testDigest4,
						},
					},
					testArtifact4: {
						{
							Digest: testDigest5,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage2,
					},
					testDigest3: {
						Description: validMessage3,
					},
					testDigest4: {
						Description: validMessage4,
					},
					testDigest5: {
						Description: validMessage5,
					},
				},
			},
			},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage3,
							},
						},
						ArtifactReports: []*ValidationReport{},
					},
					{
						Results: []*VerificationResult{
							{
								Description: validMessage2,
							},
						},
						ArtifactReports: []*ValidationReport{{
							Results: []*VerificationResult{
								{
									Description: validMessage4,
								},
							},
							ArtifactReports: []*ValidationReport{{
								Results: []*VerificationResult{
									{
										Description: validMessage5,
									},
								},
								ArtifactReports: []*ValidationReport{},
							}},
						}},
					},
				}},
			wantErr: false,
		},
		{
			name: "Verifier pruned for artifact with digest: testDigest2",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
						{
							Digest: testDigest3,
						},
					},
					testArtifact2: {
						{
							Digest: testDigest4,
						},
					},
					testArtifact4: {
						{
							Digest: testDigest5,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage2,
					},
					testDigest3: {
						Description: validMessage3,
					},
					testDigest4: {
						Description: validMessage4,
					},
					testDigest5: {
						Description: validMessage5,
					},
				},
			},
			},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					verifierPrunedDigest: testDigest2,
				},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage3,
							},
						},
						ArtifactReports: []*ValidationReport{},
					},
					{
						ArtifactReports: []*ValidationReport{{
							Results: []*VerificationResult{
								{
									Description: validMessage4,
								},
							},
							ArtifactReports: []*ValidationReport{{
								Results: []*VerificationResult{
									{
										Description: validMessage5,
									},
								},
								ArtifactReports: []*ValidationReport{},
							}},
						}},
					},
				}},
			wantErr: false,
		},
		{
			name: "Artifact pruned for artifact with digest: testDigest2",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
						{
							Digest: testDigest3,
						},
					},
					testArtifact2: {
						{
							Digest: testDigest4,
						},
					},
					testArtifact4: {
						{
							Digest: testDigest5,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage2,
					},
					testDigest3: {
						Description: validMessage3,
					},
					testDigest4: {
						Description: validMessage4,
					},
					testDigest5: {
						Description: validMessage5,
					},
				},
			},
			},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					artifactPrunedDigest: testDigest2,
				},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage3,
							},
						},
						ArtifactReports: []*ValidationReport{},
					},
					{
						ArtifactReports: []*ValidationReport{{
							Results: []*VerificationResult{
								{
									Description: validMessage4,
								},
							},
							ArtifactReports: []*ValidationReport{{
								Results: []*VerificationResult{
									{
										Description: validMessage5,
									},
								},
								ArtifactReports: []*ValidationReport{},
							}},
						}},
					},
				}},
			wantErr: false,
		},
		{
			name: "Subject pruned for subject with digest: testDigest2",
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{
							Digest: testDigest2,
						},
						{
							Digest: testDigest3,
						},
					},
					testArtifact2: {
						{
							Digest: testDigest4,
						},
					},
					testArtifact4: {
						{
							Digest: testDigest5,
						},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {
						Description: validMessage2,
					},
					testDigest3: {
						Description: validMessage3,
					},
					testDigest4: {
						Description: validMessage4,
					},
					testDigest5: {
						Description: validMessage5,
					},
				},
			},
			},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					subjectPrunedDigest: testDigest2,
				},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{
							{
								Description: validMessage3,
							},
						},
						ArtifactReports: []*ValidationReport{},
					},
					{
						Results: []*VerificationResult{
							{
								Description: validMessage2,
							},
						},
						ArtifactReports: []*ValidationReport{},
					},
				}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, _ := NewExecutor(tt.store, tt.verifiers, tt.policyEnforcer, 1)
			got, err := executor.ValidateArtifact(context.Background(), tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateArtifact() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !sameValidationResult(got, tt.want) {
				t.Errorf("ValidateArtifact() = %v, want %v", got, tt.want)
			}
		})
	}
}

func sameValidationResult(result1, result2 *ValidationResult) bool {
	if result1 == nil && result2 == nil {
		return true
	}
	if result1 == nil || result2 == nil {
		return false
	}

	if result1.Succeeded != result2.Succeeded {
		return false
	}
	if len(result1.ArtifactReports) != len(result2.ArtifactReports) {
		return false
	}
	for _, report := range result1.ArtifactReports {
		hasSameReport := false
		for _, report2 := range result2.ArtifactReports {
			if sameArtifactValidationReport(report, report2) {
				hasSameReport = true
				break
			}
		}
		if !hasSameReport {
			return false
		}
	}
	return true
}

func sameArtifactValidationReport(report1, report2 *ValidationReport) bool {
	if len(report1.Results) != len(report2.Results) {
		return false
	}
	for _, verifierReport := range report1.Results {
		hasSameReport := false
		for _, verifierReport2 := range report2.Results {
			if sameVerifierReport(verifierReport, verifierReport2) {
				hasSameReport = true
				break
			}
		}
		if !hasSameReport {
			return false
		}
	}
	if len(report1.ArtifactReports) != len(report2.ArtifactReports) {
		return false
	}
	for _, nestedReport := range report1.ArtifactReports {
		hasSameReport := false
		for _, nestedReport2 := range report2.ArtifactReports {
			if sameArtifactValidationReport(nestedReport, nestedReport2) {
				hasSameReport = true
				break
			}
		}
		if !hasSameReport {
			return false
		}
	}
	return true
}

func sameVerifierReport(report1, report2 *VerificationResult) bool {
	return report1.Err == report2.Err && report1.Description == report2.Description
}

func TestValidateExecutorSetup(t *testing.T) {
	tests := []struct {
		name      string
		store     Store
		verifiers []Verifier
		wantErr   bool
	}{
		{
			name:      "Store is not set",
			store:     nil,
			verifiers: []Verifier{&mockVerifier{}},
			wantErr:   true,
		},
		{
			name:      "Verifiers are not set",
			store:     &mockStore{},
			verifiers: nil,
			wantErr:   true,
		},
		{
			name:      "All components are set",
			store:     &mockStore{},
			verifiers: []Verifier{&mockVerifier{}},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExecutorSetup(tt.store, tt.verifiers)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateExecutorSetup() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateArtifact_SBoMNotConfigured_WithThresholdPolicy(t *testing.T) {
	policy := &ThresholdPolicyRule{
		Threshold: 1,
		Rules: []*ThresholdPolicyRule{
			{
				// only configured the verifier for signature
				Verifier: "sig-verifier",
			},
		},
	}

	enforcer, err := NewThresholdPolicyEnforcer(policy)
	if err != nil {
		t.Fatalf("Failed to create ThresholdPolicyEnforcer: %v", err)
	}

	store := &mockStore{
		tagToDesc: map[string]ocispec.Descriptor{
			testImage: {
				Digest:       testDigest1,
				ArtifactType: "application/vnd.oci.image.manifest.v1+json",
			},
		},
		// Referrers structure:
		// testImage
		// ├── testArtifact3 (SBoM)
		// │   └── testArtifact4 (sig)
		// │       └── testArtifact5
		// └── testArtifact2 (sig)
		digestToReferrers: map[string][]ocispec.Descriptor{
			testArtifact1: {
				{
					Digest:       testDigest3, // SBoM
					ArtifactType: artifactTypeSBoM,
				},
				{
					Digest:       testDigest2, // sig
					ArtifactType: artifactTypeSig,
				},
			},
			testArtifact3: {
				{
					Digest:       testDigest4, // sig
					ArtifactType: artifactTypeSig,
				},
			},
			testArtifact4: {
				{
					Digest: testDigest5,
				},
			},
		},
	}

	verifiers := []Verifier{
		&mockVerifier{
			// verifier for signature
			name:            "sig-verifier",
			verifiableTypes: []string{artifactTypeSig},
			verifyResult: map[string]*VerificationResult{
				testDigest2: {
					Description: validMessage2,
					Verifier: &mockVerifier{
						name: "sig-verifier",
					},
				},
			},
		},
		// No verifier for SBoM (artifactTypeSBoM) is configured
	}

	executor, err := NewExecutor(store, verifiers, enforcer, 1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	opts := ValidateArtifactOptions{
		Subject: testImage,
	}

	got, err := executor.ValidateArtifact(context.Background(), opts)
	if err != nil {
		t.Fatalf("ValidateArtifact() error = %v, wantErr false", err)
	}

	want := &ValidationResult{
		Succeeded: true,
		ArtifactReports: []*ValidationReport{
			{
				// empty result for SBoM as no verifier is configured
				Results:         []*VerificationResult{},
				ArtifactReports: []*ValidationReport{},
			},
			{
				Results: []*VerificationResult{
					{Description: validMessage2},
				},
				ArtifactReports: []*ValidationReport{},
			},
		},
	}

	if !sameValidationResult(got, want) {
		t.Errorf("ValidateArtifact() got = %v, want %v", got, want)
	}
}

func TestValidateArtifact_SubjectPrunedWithPreviousVerifierReport(t *testing.T) {
	policy := &ThresholdPolicyRule{
		Threshold: 1,
		Rules: []*ThresholdPolicyRule{
			{
				Verifier: "sig-verifier",
			},
		},
	}

	enforcer, err := NewThresholdPolicyEnforcer(policy)
	if err != nil {
		t.Fatalf("Failed to create ThresholdPolicyEnforcer: %v", err)
	}

	store := &mockStore{
		tagToDesc: map[string]ocispec.Descriptor{
			testImage: {
				Digest:       testDigest1,
				ArtifactType: "application/vnd.oci.image.manifest.v1+json",
			},
		},
		// Referrers structure:
		// testImage
		// └── testArtifact2 (sig)
		digestToReferrers: map[string][]ocispec.Descriptor{
			testArtifact1: {
				{
					Digest:       testDigest2, // sig
					ArtifactType: artifactTypeSig,
				},
			},
		},
	}

	// more than one verifier for signature
	verifiers := []Verifier{
		&mockVerifier{
			name:            "sig-verifier",
			verifiableTypes: []string{artifactTypeSig},
			verifyResult: map[string]*VerificationResult{
				testDigest2: {
					Description: validMessage2,
					Verifier: &mockVerifier{
						name: "sig-verifier",
					},
				},
			},
		},
		&mockVerifier{
			name:            "sig-verifier2",
			verifiableTypes: []string{artifactTypeSig},
			verifyResult: map[string]*VerificationResult{
				testDigest2: {
					Description: "other message",
					Verifier: &mockVerifier{
						name: "sig-verifier2",
					},
				},
			},
		},
	}

	executor, err := NewExecutor(store, verifiers, enforcer, 1)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	opts := ValidateArtifactOptions{
		Subject: testImage,
	}

	got, err := executor.ValidateArtifact(context.Background(), opts)
	if err != nil {
		t.Fatalf("ValidateArtifact() error = %v, wantErr false", err)
	}

	want := &ValidationResult{
		Succeeded: true,
		ArtifactReports: []*ValidationReport{
			{
				Results: []*VerificationResult{
					{Description: validMessage2},
				},
				ArtifactReports: []*ValidationReport{},
			},
		},
	}

	if !sameValidationResult(got, want) {
		t.Errorf("ValidateArtifact() got = %v, want %v", got, want)
	}
}

func TestValidateArtifact_ConcurrentExecution(t *testing.T) {
	tests := []struct {
		name           string
		maxConcurrency int
		opts           ValidateArtifactOptions
		store          Store
		verifiers      []Verifier
		policyEnforcer PolicyEnforcer
		want           *ValidationResult
		wantErr        bool
	}{
		{
			name:           "Concurrent execution with 4 goroutines - multiple artifacts",
			maxConcurrency: 4,
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{Digest: testDigest2},
						{Digest: testDigest3},
						{Digest: testDigest4},
						{Digest: testDigest5},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {Description: validMessage2},
					testDigest3: {Description: validMessage3},
					testDigest4: {Description: validMessage4},
					testDigest5: {Description: validMessage5},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{Results: []*VerificationResult{{Description: validMessage2}}},
					{Results: []*VerificationResult{{Description: validMessage3}}},
					{Results: []*VerificationResult{{Description: validMessage4}}},
					{Results: []*VerificationResult{{Description: validMessage5}}},
				},
			},
			wantErr: false,
		},
		{
			name:           "Concurrent execution with 4 goroutines - nested artifacts",
			maxConcurrency: 4,
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{Digest: testDigest2},
						{Digest: testDigest3},
					},
					testArtifact2: {
						{Digest: testDigest4},
					},
					testArtifact3: {
						{Digest: testDigest5},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {Description: validMessage2},
					testDigest3: {Description: validMessage3},
					testDigest4: {Description: validMessage4},
					testDigest5: {Description: validMessage5},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{{Description: validMessage2}},
						ArtifactReports: []*ValidationReport{
							{Results: []*VerificationResult{{Description: validMessage4}}},
						},
					},
					{
						Results: []*VerificationResult{{Description: validMessage3}},
						ArtifactReports: []*ValidationReport{
							{Results: []*VerificationResult{{Description: validMessage5}}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:           "Concurrent execution with 4 goroutines - error in one goroutine",
			maxConcurrency: 4,
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{Digest: testDigest2},
						{Digest: testDigest3},
						{Digest: testDigest4},
						{Digest: testDigest5},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				// Missing result for testDigest3 will cause an error
				verifyResult: map[string]*VerificationResult{
					testDigest2: {Description: validMessage2},
					// testDigest4: {Description: validMessage4},
					// testDigest5: {Description: validMessage5},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:           "Concurrent execution with 4 goroutines - evaluator add result error",
			maxConcurrency: 4,
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{Digest: testDigest2},
						{Digest: testDigest3},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {Description: validMessage2},
					testDigest3: {Description: validMessage3},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{
					returnAddResultErr: true,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:           "Concurrent execution with 4 goroutines - complex artifact tree",
			maxConcurrency: 4,
			opts: ValidateArtifactOptions{
				Subject: testImage,
			},
			store: &mockStore{
				tagToDesc: map[string]ocispec.Descriptor{
					testImage: {
						Digest: testDigest1,
					},
				},
				// Complex tree structure to stress test concurrent execution
				digestToReferrers: map[string][]ocispec.Descriptor{
					testArtifact1: {
						{Digest: testDigest2},
						{Digest: testDigest3},
					},
					testArtifact2: {
						{Digest: testDigest4},
						{Digest: testDigest5},
					},
					testArtifact3: {
						{Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"},
						{Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222"},
					},
					"test-registry/test-repo@sha256:1111111111111111111111111111111111111111111111111111111111111111": {
						{Digest: "sha256:3333333333333333333333333333333333333333333333333333333333333333"},
					},
				},
			},
			verifiers: []Verifier{&mockVerifier{
				verifiable: true,
				verifyResult: map[string]*VerificationResult{
					testDigest2: {Description: validMessage2},
					testDigest3: {Description: validMessage3},
					testDigest4: {Description: validMessage4},
					testDigest5: {Description: validMessage5},
					"sha256:1111111111111111111111111111111111111111111111111111111111111111": {Description: "valid message 6"},
					"sha256:2222222222222222222222222222222222222222222222222222222222222222": {Description: "valid message 7"},
					"sha256:3333333333333333333333333333333333333333333333333333333333333333": {Description: "valid message 8"},
				},
			}},
			policyEnforcer: &mockPolicyEnforcer{
				evaluator: &mockEvaluator{},
			},
			want: &ValidationResult{
				Succeeded: true,
				ArtifactReports: []*ValidationReport{
					{
						Results: []*VerificationResult{{Description: validMessage2}},
						ArtifactReports: []*ValidationReport{
							{Results: []*VerificationResult{{Description: validMessage4}}},
							{Results: []*VerificationResult{{Description: validMessage5}}},
						},
					},
					{
						Results: []*VerificationResult{{Description: validMessage3}},
						ArtifactReports: []*ValidationReport{
							{
								Results: []*VerificationResult{{Description: "valid message 6"}},
								ArtifactReports: []*ValidationReport{
									{Results: []*VerificationResult{{Description: "valid message 8"}}},
								},
							},
							{Results: []*VerificationResult{{Description: "valid message 7"}}},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := NewExecutor(tt.store, tt.verifiers, tt.policyEnforcer, tt.maxConcurrency)
			if err != nil {
				t.Fatalf("Failed to create executor: %v", err)
			}

			got, err := executor.ValidateArtifact(context.Background(), tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateArtifact() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !sameValidationResult(got, tt.want) {
				t.Errorf("ValidateArtifact() = %v, want %v", got, tt.want)
			}

			// Verify that the executor was configured with the correct concurrency
			if executor.MaxConcurrency != tt.maxConcurrency {
				t.Errorf("Expected MaxConcurrency = %d, got %d", tt.maxConcurrency, executor.MaxConcurrency)
			}
		})
	}
}

// func TestValidateArtifact_ConcurrentRaceConditions(t *testing.T) {
// 	// This test specifically checks for race conditions by running the same
// 	// validation multiple times concurrently
// 	store := &mockStore{
// 		tagToDesc: map[string]ocispec.Descriptor{
// 			testImage: {
// 				Digest: testDigest1,
// 			},
// 		},
// 		digestToReferrers: map[string][]ocispec.Descriptor{
// 			testArtifact1: {
// 				{Digest: testDigest2},
// 				{Digest: testDigest3},
// 				{Digest: testDigest4},
// 				{Digest: testDigest5},
// 			},
// 		},
// 	}

// 	verifiers := []Verifier{&mockVerifier{
// 		verifiable: true,
// 		verifyResult: map[string]*VerificationResult{
// 			testDigest2: {Description: validMessage2},
// 			testDigest3: {Description: validMessage3},
// 			testDigest4: {Description: validMessage4},
// 			testDigest5: {Description: validMessage5},
// 		},
// 	}}

// 	policyEnforcer := &mockPolicyEnforcer{
// 		evaluator: &mockEvaluator{},
// 	}

// 	executor, err := NewExecutor(store, verifiers, policyEnforcer, 4)
// 	if err != nil {
// 		t.Fatalf("Failed to create executor: %v", err)
// 	}

// 	opts := ValidateArtifactOptions{
// 		Subject: testImage,
// 	}

// 	// Run multiple validations concurrently to test for race conditions
// 	const numConcurrentValidations = 10
// 	var wg sync.WaitGroup
// 	results := make([]*ValidationResult, numConcurrentValidations)
// 	errors := make([]error, numConcurrentValidations)

// 	for i := 0; i < numConcurrentValidations; i++ {
// 		wg.Add(1)
// 		go func(index int) {
// 			defer wg.Done()
// 			result, err := executor.ValidateArtifact(context.Background(), opts)
// 			results[index] = result
// 			errors[index] = err
// 		}(i)
// 	}

// 	wg.Wait()

// 	// Verify all validations succeeded
// 	for i := 0; i < numConcurrentValidations; i++ {
// 		if errors[i] != nil {
// 			t.Errorf("Validation %d failed with error: %v", i, errors[i])
// 		}
// 		if results[i] == nil {
// 			t.Errorf("Validation %d returned nil result", i)
// 			continue
// 		}
// 		if !results[i].Succeeded {
// 			t.Errorf("Validation %d failed", i)
// 		}
// 		if len(results[i].ArtifactReports) != 4 {
// 			t.Errorf("Validation %d returned %d reports, expected 4", i, len(results[i].ArtifactReports))
// 		}
// 	}

// 	// Verify all results are consistent
// 	for i := 1; i < numConcurrentValidations; i++ {
// 		if !sameValidationResult(results[0], results[i]) {
// 			t.Errorf("Results are inconsistent between validation 0 and %d", i)
// 		}
// 	}
// }
