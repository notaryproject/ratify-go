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
	"sync/atomic"

	ratiSync "github.com/notaryproject/ratify-go/internal/sync"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/semaphore"
	"oras.land/oras-go/v2/registry"
)

// defaultConcurrency is the default value of [Executor.concurrency]
const defaultConcurrency = 3

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

	// Concurrency limits the maximum number of concurrent execution per
	// validation request. If less than or equal to 0, a default (currently 3)
	// is used.
	Concurrency int
}

// NewExecutor creates a new executor with the given verifiers, store, and
// policy enforcer.
func NewExecutor(store Store, verifiers []Verifier, policyEnforcer PolicyEnforcer) (*Executor, error) {
	if err := validateExecutorSetup(store, verifiers, defaultConcurrency); err != nil {
		return nil, err
	}

	return &Executor{
		Store:          store,
		Verifiers:      verifiers,
		PolicyEnforcer: policyEnforcer,
		Concurrency:    defaultConcurrency,
	}, nil
}

// ValidateArtifact returns the result of verifying an artifact.
func (e *Executor) ValidateArtifact(ctx context.Context, opts ValidateArtifactOptions) (*ValidationResult, error) {
	if err := validateExecutorSetup(e.Store, e.Verifiers, e.Concurrency); err != nil {
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
	// Only resolve the root subject reference.
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

	rootTask := &executorTask{
		artifact:     ref,
		artifactDesc: desc,
		subjectReport: &ValidationReport{
			Artifact: desc,
		},
	}
	return e.processTasks(ctx, rootTask, repo, opts.ReferenceTypes, evaluator)
}

// processTasks processes the tasks in the worker manager and returns the
// aggregated reports and the evaluator.
func (e *Executor) processTasks(ctx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator) ([]*ValidationReport, Evaluator, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr atomic.Pointer[error]
	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(e.Concurrency) - 1)
	sched := newScheduler(sem, &wg, &firstErr, cancel)
	stack := ratiSync.NewStack[*executorTask]()
	stack.Push(task)

	func() {
		for {
			task, ok := stack.Pop()
			if !ok {
				// No more tasks to process, break the loop.
				return
			}

			select {
			case <-childCtx.Done():
				stack.Done()
				return
			default:
			}

			if err := sched.execute(func() error {
				return e.verifySubjectAgainstReferrers(childCtx, task, repo, referenceTypes, evaluator, sem, stack)
			}, stack.Done); err != nil {
				firstErr.CompareAndSwap(nil, &err)
				return
			}
		}
	}()

	if err := waitWithContext(childCtx, &wg); err != nil {
		return nil, nil, err
	}
	if err := firstErr.Load(); err != nil {
		return nil, nil, *err
	}
	return task.subjectReport.ArtifactReports, evaluator, nil
}

// verifySubjectAgainstReferrers verifies the subject artifact against all
// referrers in the store and produces new tasks for each referrer.
func (e *Executor) verifySubjectAgainstReferrers(ctx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator, sem *semaphore.Weighted, stack *ratiSync.Stack[*executorTask]) error {
	artifact := task.artifact.String()
	artifactReports := ratiSync.NewSlice[*ValidationReport]()
	err := e.Store.ListReferrers(ctx, artifact, referenceTypes, func(referrers []ocispec.Descriptor) error {
		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var firstErr atomic.Pointer[error]
		var wg sync.WaitGroup
		sched := newScheduler(sem, &wg, &firstErr, cancel)

		for _, referrer := range referrers {
			select {
			case <-childCtx.Done():
				return childCtx.Err()
			default:
			}

			if err := sched.execute(func() error {
				return e.processReferrer(childCtx, task, repo, evaluator, referrer, artifact, sem, stack, artifactReports)
			}, nil); err != nil {
				// Handle synchronous execution error
				firstErr.CompareAndSwap(nil, &err)
				break
			}
		}
		if err := waitWithContext(childCtx, &wg); err != nil {
			// Context was cancelled, return early
			return err
		}
		if err := firstErr.Load(); err != nil {
			return *err
		}
		return nil
	})

	if err != nil {
		if err != errSubjectPruned {
			return fmt.Errorf("failed to verify referrers for artifact %s: %w", artifact, err)
		}
	}
	if evaluator != nil {
		if err := evaluator.Commit(ctx, task.artifactDesc.Digest.String()); err != nil {
			return fmt.Errorf("failed to commit the artifact %s: %w", artifact, err)
		}
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	task.subjectReport.ArtifactReports = append(task.subjectReport.ArtifactReports, artifactReports.Get()...)

	return nil
}

// processReferrer processes a single referrer artifact, verifies it, and
// creates a new task for it if necessary.
func (e *Executor) processReferrer(ctx context.Context, task *executorTask, repo string, evaluator Evaluator, referrer ocispec.Descriptor, artifact string, sem *semaphore.Weighted, stack *ratiSync.Stack[*executorTask], artifactReports *ratiSync.Slice[*ValidationReport]) error {
	results, err := e.verifyArtifact(ctx, repo, task.artifactDesc, referrer, evaluator, sem)
	if err != nil {
		if errors.Is(err, errSubjectPruned) && len(results) > 0 {
			// it is possible that one or some verifiers' reports in the
			// results and the next verifier triggers the subject pruned state,
			// so the results are not empty.
			artifactReport := &ValidationReport{
				Subject:  artifact,
				Results:  results,
				Artifact: referrer,
			}
			artifactReports.Add(artifactReport)
		}
		return err
	}

	artifactReport := &ValidationReport{
		Subject:  artifact,
		Results:  results,
		Artifact: referrer,
	}
	artifactReports.Add(artifactReport)

	referrerArtifact := task.artifact
	referrerArtifact.Reference = referrer.Digest.String()
	newTask := &executorTask{
		artifact:      referrerArtifact,
		artifactDesc:  referrer,
		subjectReport: artifactReport,
	}
	stack.Push(newTask)
	return nil
}

// verifyArtifact verifies the artifact by all configured verifiers and returns
// error if any of the verifier fails.
func (e *Executor) verifyArtifact(ctx context.Context, repo string, subjectDesc, artifact ocispec.Descriptor, evaluator Evaluator, sem *semaphore.Weighted) ([]*VerificationResult, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var firstErr atomic.Pointer[error]
	sched := newScheduler(sem, &wg, &firstErr, cancel)
	verificationResults := ratiSync.NewSlice[*VerificationResult]()

	for _, verifier := range e.Verifiers {
		select {
		case <-childCtx.Done():
			return verificationResults.Get(), childCtx.Err()
		default:
		}

		if !verifier.Verifiable(artifact) {
			continue
		}

		if err := sched.execute(func() error {
			return e.processVerifier(childCtx, subjectDesc, repo, artifact, verifier, evaluator, verificationResults)
		}, nil); err != nil {
			// Handle synchronous execution error
			firstErr.CompareAndSwap(nil, &err)
			break
		}
	}

	if err := waitWithContext(childCtx, &wg); err != nil {
		// Context was cancelled, return early
		return verificationResults.Get(), err
	}
	if err := firstErr.Load(); err != nil {
		return verificationResults.Get(), *err
	}

	return verificationResults.Get(), nil
}

// processVerifier processes a single verifier, verifies the subject against
// the artifact, and adds the report to the evaluator if available.
func (e *Executor) processVerifier(ctx context.Context, subjectDesc ocispec.Descriptor, repo string, artifact ocispec.Descriptor, verifier Verifier, evaluator Evaluator, verificationResults *ratiSync.Slice[*VerificationResult]) error {
	if evaluator != nil {
		prunedState, err := evaluator.Pruned(ctx, subjectDesc.Digest.String(), artifact.Digest.String(), verifier.Name())
		if err != nil {
			return fmt.Errorf("failed to check if verifier: %s is required to verify subject: %s, against artifact: %s, err: %w", verifier.Name(), subjectDesc.Digest, artifact.Digest, err)
		}
		switch prunedState {
		case PrunedStateVerifierPruned:
			// Skip this verifier if it's not required.
			return nil
		case PrunedStateArtifactPruned:
			// Skip remaining verifiers if the artifact is not required.
			return nil
		case PrunedStateSubjectPruned:
			// Skip remaining verifiers and return `errSubjectPruned` to
			// notify `ListReferrers`stop processing.
			return errSubjectPruned
		default:
			// do nothing if it's not pruned.
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Verify the subject artifact against the referrer artifact.
	verifierReport, err := verifier.Verify(ctx, &VerifyOptions{
		Store:              e.Store,
		Repository:         repo,
		SubjectDescriptor:  subjectDesc,
		ArtifactDescriptor: artifact,
	})
	if err != nil {
		return fmt.Errorf("failed to verify artifact %s@%s with verifier %s: %w", repo, subjectDesc.Digest, verifier.Name(), err)
	}

	if evaluator != nil {
		if err := evaluator.AddResult(ctx, subjectDesc.Digest.String(), artifact.Digest.String(), verifierReport); err != nil {
			return fmt.Errorf("failed to add verifier report for artifact %s@%s verified by verifier %s: %w", repo, subjectDesc.Digest, verifier.Name(), err)
		}
	}
	verificationResults.Add(verifierReport)
	return nil
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

// executorTask is a struct that represents a task that verifies an artifact by
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

	mu sync.Mutex
}

func validateExecutorSetup(store Store, verifiers []Verifier, concurrency int) error {
	if store == nil {
		return fmt.Errorf("store must be configured")
	}
	if len(verifiers) == 0 {
		return fmt.Errorf("at least one verifier must be configured")
	}
	if concurrency < 0 {
		return fmt.Errorf("concurrency must be greater than or equal to 0, got %d", concurrency)
	}
	return nil
}

// scheduler handles the pattern of running functions either concurrently or
// synchronously
type scheduler struct {
	sem      *semaphore.Weighted
	wg       *sync.WaitGroup
	firstErr *atomic.Pointer[error] // holds the first error encountered during execution
	cancel   func()
}

func newScheduler(sem *semaphore.Weighted, wg *sync.WaitGroup, firstErr *atomic.Pointer[error], cancel func()) *scheduler {
	return &scheduler{
		sem:      sem,
		wg:       wg,
		firstErr: firstErr,
		cancel:   cancel,
	}
}

// execute runs the function either concurrently (if concurrency slot available)
// or synchronously.
// cleanup is called in the defer of the goroutine (for concurrent execution) or
// after sync execution.
func (s *scheduler) execute(fn func() error, cleanup func()) error {
	if s.sem.TryAcquire(1) {
		s.wg.Add(1)
		go func() {
			defer func() {
				s.sem.Release(1)
				s.wg.Done()
				if cleanup != nil {
					cleanup()
				}
			}()

			if err := fn(); err != nil {
				s.firstErr.CompareAndSwap(nil, &err)
				s.cancel()
			}
		}()
		return nil
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	if err := fn(); err != nil {
		s.firstErr.CompareAndSwap(nil, &err)
		s.cancel()
		return err
	}
	return nil
}

// waitWithContext waits for either the WaitGroup to complete or the context to
// be cancelled. It returns nil if WaitGroup completed successfully, or an error
// if the context was cancelled and an error occurred.
func waitWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if err := ctx.Err(); !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}
}
