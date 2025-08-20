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
	"slices"

	"github.com/notaryproject/ratify-go/internal/syncutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry"
)

// errSubjectPruned is returned when the evaluator does not need given subject
// to be verified to make a decision by [Evaluator.Pruned].
var errSubjectPruned = errors.New("evaluator sub-graph is pruned for the subject")

// ValidateArtifactOptions describes the artifact validation options.
type ValidateArtifactOptions struct {
	// Subject is the reference of the artifact to be validated. Required.
	Subject string

	// ReferenceTypes is a list of reference types that should be verified
	// against in associated artifacts. Empty list means all artifacts should be
	// verified. Optional.
	ReferenceTypes []string
}

// ValidationResult aggregates verifier reports and the final verification
// result evaluated by the policy enforcer.
type ValidationResult struct {
	// Succeeded represents the outcome determined by the policy enforcer based
	// on the aggregated verifier reports. And if an error occurs during the
	// validation process prior to policy evaluation, it will be set to `false`.
	// If the policy enforcer is not set in the executor, this field will be set
	// to `false`. In such cases, this field should be ignored. Required.
	Succeeded bool

	// ArtifactReports is aggregated reports of verifying associated artifacts.
	// This field can be nil if an error occured during validation or no reports
	// were generated. Optional.
	ArtifactReports []*ValidationReport
}

// Executor is defined to validate artifacts.
type Executor struct {
	// Executor should configure exactly one store to fetch supply chain
	// content. Required.
	Store Store

	// Executor could use multiple verifiers to validate artifacts. Required.
	Verifiers []Verifier

	// Executor should have at most one policy enforcer to evalute reports. If
	// not set, the validation result will be returned without evaluation.
	// Optional.
	PolicyEnforcer PolicyEnforcer

	// MaxWorkers is the maximum number of concurrent workers that can be used
	// to validate artifacts. This limit is used to create worker pools.
	//
	// note: each call to Executor.ValidateArtifact will create new worker pools
	// based on MaxWorkers.
	MaxWorkers int
}

// NewExecutor creates a new executor with the given verifiers, store, and
// policy enforcer.
//
// maxWorkers is the maximum number of concurrent workers that can be
// executed. It must be greater or equal to 1.
func NewExecutor(store Store, verifiers []Verifier, policyEnforcer PolicyEnforcer, maxWorkers int) (*Executor, error) {
	if err := validateExecutorSetup(store, verifiers, maxWorkers); err != nil {
		return nil, err
	}

	return &Executor{
		Store:          store,
		Verifiers:      verifiers,
		PolicyEnforcer: policyEnforcer,
		MaxWorkers:     maxWorkers,
	}, nil
}

// ValidateArtifact returns the result of verifying an artifact.
func (e *Executor) ValidateArtifact(ctx context.Context, opts ValidateArtifactOptions) (*ValidationResult, error) {
	if err := validateExecutorSetup(e.Store, e.Verifiers, e.MaxWorkers); err != nil {
		return nil, err
	}

	aggregatedVerifierReports, evaluator, err := e.aggregateVerifierReports(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to generate and aggregate verifier reports: %w", err)
	}

	if evaluator == nil {
		return &ValidationResult{
			Succeeded:       false,
			ArtifactReports: aggregatedVerifierReports,
		}, nil
	}

	decision, err := evaluator.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate verifier reports: %w", err)
	}

	return &ValidationResult{
		Succeeded:       decision,
		ArtifactReports: aggregatedVerifierReports,
	}, nil
}

// aggregateVerifierReports generates and aggregates all verifier reports.
func (e *Executor) aggregateVerifierReports(ctx context.Context, opts ValidateArtifactOptions) ([]*ValidationReport, Evaluator, error) {
	// only resolve the root subject reference.
	ref, desc, err := e.resolveSubject(ctx, opts.Subject)
	if err != nil {
		return nil, nil, err
	}
	repo := ref.Registry + "/" + ref.Repository

	var evaluator Evaluator
	if e.PolicyEnforcer != nil {
		evaluator, err = e.PolicyEnforcer.Evaluator(ctx, ref.Reference)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create a new evaluator: %w", err)
		}
	}

	// create worker pools for referrer and verifier tasks
	// all workers should be completed before return, so defer closing the pools.
	referrerPoolSlots := make(syncutil.PoolSlots, e.MaxWorkers)
	defer close(referrerPoolSlots)
	verifierPoolSlots := make(syncutil.PoolSlots, e.MaxWorkers)
	defer close(verifierPoolSlots)

	rootTask := &executorTask{
		artifact:     ref,
		artifactDesc: desc,
		subjectReport: &ValidationReport{
			Artifact: desc,
		},
	}

	artifactTaskPool, ctx := syncutil.NewTaskPool(ctx, e.MaxWorkers)
	artifactTaskPool.Submit(func() error {
		return e.verifySubjectAgainstReferrers(ctx, rootTask, repo, opts.ReferenceTypes, evaluator, artifactTaskPool, referrerPoolSlots, verifierPoolSlots)
	})

	if err := artifactTaskPool.Wait(); err != nil {
		return nil, nil, fmt.Errorf("failed to verify subject artifact %s: %w", opts.Subject, err)
	}

	return rootTask.subjectReport.ArtifactReports, evaluator, nil
}

// verifySubjectAgainstReferrers verifies the subject artifact against all
// referrers in the store and produces new tasks for each referrer.
func (e *Executor) verifySubjectAgainstReferrers(ctx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator, artifactTaskPool *syncutil.TaskPool, referrerPoolSlots, verifierPoolSlots syncutil.PoolSlots) error {
	referrerWorkerPool, poolctx := syncutil.NewSharedWorkerPool[*ValidationReport](ctx, referrerPoolSlots)
	artifact := task.artifact.String()

	// We need to verify the artifact against its required referrer artifacts.
	// artifactReports is used to store the validation reports of those
	// referrer artifacts.
	err := e.Store.ListReferrers(poolctx, artifact, referenceTypes, func(referrers []ocispec.Descriptor) error {
		for i := range referrers {
			referrer := referrers[i]
			if err := referrerWorkerPool.Go(func() (*ValidationReport, error) {
				results, err := e.verifyArtifact(poolctx, repo, task.artifactDesc, referrer, evaluator, verifierPoolSlots)
				if err != nil {
					if errors.Is(err, errSubjectPruned) && len(results) > 0 {
						// it is possible that one or some verifiers' reports in the
						// results and the next verifier triggers the subject pruned state,
						// so the results are not empty.
						referrerReport := &ValidationReport{
							Subject:  artifact,
							Results:  results,
							Artifact: referrer,
						}
						return referrerReport, errSubjectPruned
					}
					return nil, fmt.Errorf("failed to verify referrer artifact %s against subject artifact %s: %w", referrer.Digest, artifact, err)
				}

				return &ValidationReport{
					Subject:  artifact,
					Results:  results,
					Artifact: referrer,
				}, nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errSubjectPruned) {
		_, workerError := referrerWorkerPool.Wait()
		if workerError != nil {
			return fmt.Errorf("failed to list referrers for artifact %s: %w", artifact, workerError)
		}
		return fmt.Errorf("failed to list referrers for artifact %s: %w", artifact, err)
	}

	isSubjectPruned := false
	referrerReports, err := referrerWorkerPool.Wait()
	if err != nil {
		if !errors.Is(err, errSubjectPruned) {
			return fmt.Errorf("failed to verify referrers for artifact %s: %w", artifact, err)
		}
		isSubjectPruned = true
	}

	if evaluator != nil {
		if err := evaluator.Commit(poolctx, task.artifactDesc.Digest.String()); err != nil {
			return fmt.Errorf("failed to commit the artifact %s: %w", artifact, err)
		}
	}

	// process reports
	for _, referrerReport := range referrerReports {
		if referrerReport == nil {
			continue
		}
		// add the referrer report
		task.subjectReport.ArtifactReports = append(task.subjectReport.ArtifactReports, referrerReport)

		if !isSubjectPruned {
			// add a new task
			referrerArtifact := task.artifact
			referrerArtifact.Reference = referrerReport.Artifact.Digest.String()
			newTask := &executorTask{
				artifact:      referrerArtifact,
				artifactDesc:  referrerReport.Artifact,
				subjectReport: referrerReport,
			}
			artifactTaskPool.Submit(func() error {
				return e.verifySubjectAgainstReferrers(ctx, newTask, repo, referenceTypes, evaluator, artifactTaskPool, referrerPoolSlots, verifierPoolSlots)
			})
		}
	}
	return nil
}

// verifyArtifact verifies the artifact by all configured verifiers and returns
// error if any of the verifier fails.
func (e *Executor) verifyArtifact(ctx context.Context, repo string, subjectDesc, artifact ocispec.Descriptor, evaluator Evaluator, verifierPoolSlots syncutil.PoolSlots) ([]*VerificationResult, error) {
	verifierWorkerPool, ctx := syncutil.NewSharedWorkerPool[*VerificationResult](ctx, verifierPoolSlots)
	for i := range e.Verifiers {
		verifier := e.Verifiers[i]
		if !verifier.Verifiable(artifact) {
			continue
		}

		if err := verifierWorkerPool.Go(func() (*VerificationResult, error) {
			if evaluator != nil {
				prunedState, err := evaluator.Pruned(ctx, subjectDesc.Digest.String(), artifact.Digest.String(), verifier.Name())
				if err != nil {
					return nil, fmt.Errorf("failed to check if verifier: %s is required to verify subject: %s, against artifact: %s, err: %w", verifier.Name(), subjectDesc.Digest, artifact.Digest, err)
				}
				switch prunedState {
				case PrunedStateVerifierPruned:
					// Skip this verifier if it's not required.
					return nil, nil
				case PrunedStateSubjectPruned:
					// Skip remaining verifiers and return `errSubjectPruned` to
					// notify `ListReferrers`stop processing.
					return nil, errSubjectPruned
				default:
					// do nothing if it's not pruned.
				}
			}

			// Verify the subject artifact against the referrer artifact.
			verifierReport, err := verifier.Verify(ctx, &VerifyOptions{
				Store:              e.Store,
				Repository:         repo,
				SubjectDescriptor:  subjectDesc,
				ArtifactDescriptor: artifact,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to verify artifact %s@%s with verifier %s: %w", repo, subjectDesc.Digest, verifier.Name(), err)
			}

			if evaluator != nil {
				if err := evaluator.AddResult(ctx, subjectDesc.Digest.String(), artifact.Digest.String(), verifierReport); err != nil {
					return nil, fmt.Errorf("failed to add verifier report for artifact %s@%s verified by verifier %s: %w", repo, subjectDesc.Digest, verifier.Name(), err)
				}
			}

			return verifierReport, nil
		}); err != nil {
			// error will be handled by Wait()
			break
		}
	}
	verificationResults, err := verifierWorkerPool.Wait()
	if err != nil && !errors.Is(err, errSubjectPruned) {
		return nil, err
	}
	return slices.DeleteFunc(verificationResults, func(result *VerificationResult) bool {
		// filter out nil results
		return result == nil
	}), err
}

func (e *Executor) resolveSubject(ctx context.Context, subject string) (registry.Reference, ocispec.Descriptor, error) {
	ref, err := registry.ParseReference(subject)
	if err != nil {
		return registry.Reference{}, ocispec.Descriptor{}, fmt.Errorf("failed to parse subject reference %s: %w", subject, err)
	}

	artifactDesc, err := e.Store.Resolve(ctx, ref.String())
	if err != nil {
		return registry.Reference{}, ocispec.Descriptor{}, fmt.Errorf("failed to resolve subject reference %s: %w", ref.Reference, err)
	}
	ref.Reference = artifactDesc.Digest.String()
	return ref, artifactDesc, nil
}

// executorTask is a struct that represents a executorTask that verifies an artifact by
// the executor.
type executorTask struct {
	// artifact is the digested reference of the referrer artifact that will be
	// verified against.
	artifact registry.Reference

	// artifactDesc is the descriptor of the referrer artifact that will be
	// verified against.
	artifactDesc ocispec.Descriptor

	// subjectReport is the report of the subject artifact.
	subjectReport *ValidationReport
}

func validateExecutorSetup(store Store, verifiers []Verifier, maxWorkers int) error {
	if store == nil {
		return fmt.Errorf("store must be configured")
	}
	if len(verifiers) == 0 {
		return fmt.Errorf("at least one verifier must be configured")
	}
	if maxWorkers <= 0 {
		return fmt.Errorf("max workers (%d) must be greater than 0", maxWorkers)
	}
	return nil
}
