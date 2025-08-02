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
	"runtime"
	"sync"
	"sync/atomic"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry"
)

// errSubjectPruned is returned when the evaluator does not need given subject
// to be verified to make a decision by [Evaluator.Pruned].
var errSubjectPruned = errors.New("evaluator sub-graph is pruned for the subject")

// concurrentTaskQueue is a thread-safe task queue for concurrent processing
type concurrentTaskQueue struct {
	mu    sync.Mutex
	tasks []*executorTask
	cond  *sync.Cond
	done  bool
}

func newConcurrentTaskQueue() *concurrentTaskQueue {
	q := &concurrentTaskQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *concurrentTaskQueue) isEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) == 0 && !q.done
}

func (q *concurrentTaskQueue) push(tasks ...*executorTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.done {
		return // Don't add tasks to a closed queue
	}
	q.tasks = append(q.tasks, tasks...)
	q.cond.Broadcast()
}

func (q *concurrentTaskQueue) pop() (task *executorTask, success bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 && !q.done {
		q.cond.Wait()
	}

	if len(q.tasks) == 0 {
		return nil, false
	}

	task = q.tasks[len(q.tasks)-1]
	q.tasks = q.tasks[:len(q.tasks)-1]
	return task, true
}

func (q *concurrentTaskQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.cond.Broadcast()
}

// concurrencyController manages the available goroutine slots
type concurrencyController struct {
	semaphore chan struct{}
}

func newConcurrencyController(maxConcurrency int) *concurrencyController {
	if maxConcurrency <= 1 {
		return &concurrencyController{}
	}
	return &concurrencyController{
		semaphore: make(chan struct{}, maxConcurrency-1),
	}
}

func (c *concurrencyController) release() {
	if c.semaphore != nil {
		<-c.semaphore
	}
}

func (c *concurrencyController) tryAcquire() bool {
	if c.semaphore == nil {
		return false
	}

	select {
	case c.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

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

	// MaxConcurrency is the maximum number of goroutines that can be created
	// for each artifact validation request. If set to 1, single thread mode is
	// used. If set to 0, defaults to runtime.NumCPU().
	// Optional.
	MaxConcurrency int
}

// NewExecutor creates a new executor with the given verifiers, store, and
// policy enforcer.
func NewExecutor(store Store, verifiers []Verifier, policyEnforcer PolicyEnforcer, maxConcurrency int) (*Executor, error) {
	if err := validateExecutorSetup(store, verifiers); err != nil {
		return nil, err
	}
	if maxConcurrency < 0 {
		return nil, fmt.Errorf("maxConcurrency must be non-negative, got %d", maxConcurrency)
	}
	if maxConcurrency == 0 {
		maxConcurrency = runtime.NumCPU() // default to number of CPUs
	}

	return &Executor{
		Store:          store,
		Verifiers:      verifiers,
		PolicyEnforcer: policyEnforcer,
	}, nil
}

// ValidateArtifact returns the result of verifying an artifact.
func (e *Executor) ValidateArtifact(ctx context.Context, opts ValidateArtifactOptions) (*ValidationResult, error) {
	if err := validateExecutorSetup(e.Store, e.Verifiers); err != nil {
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

	// Enqueue the subject artifact as the first task.
	rootTask := &executorTask{
		artifact:     ref,
		artifactDesc: desc,
		subjectReport: &ValidationReport{
			Artifact: desc,
		},
	}
	return e.processVerifierReports(ctx, rootTask, repo, opts.ReferenceTypes, evaluator)
}

type validationOpts struct {
	atomicErr     atomic.Value
	activeWorkers atomic.Int64
	taskQueue     *concurrentTaskQueue
}

func (v *validationOpts) startValidation() {
	v.activeWorkers.Add(1)
}

func (v *validationOpts) endValidation() {
	v.activeWorkers.Add(-1)
	if v.taskQueue.isEmpty() && v.activeWorkers.Load() == 0 {
		v.taskQueue.close()
	}
}

func (e *Executor) processVerifierReports(parentCtx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator) ([]*ValidationReport, Evaluator, error) {
	taskQueue := newConcurrentTaskQueue()
	concurrencyController := newConcurrencyController(e.MaxConcurrency)

	taskQueue.push(task)

	baseCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	var firstErr atomic.Value
	var wg sync.WaitGroup

	opts := &validationOpts{
		taskQueue: taskQueue,
	}
LOOP:
	for {
		opts.startValidation()
		task, ok := opts.taskQueue.pop()
		if !ok {
			// No more tasks to process, break the loop.
			opts.endValidation()
			break LOOP
		}

		select {
		case <-baseCtx.Done():
			opts.endValidation()
			break LOOP
		default:
		}

		if concurrencyController.tryAcquire() {
			wg.Add(1)
			go func() {
				defer func() {
					concurrencyController.release()
					opts.endValidation()
					wg.Done()
				}()

				if err := e.verifySubjectAgainstReferrers(baseCtx, task, repo, referenceTypes, evaluator, concurrencyController, opts); err != nil {
					firstErr.Store(err)
				}
			}()
		} else {
			if err := e.verifySubjectAgainstReferrers(baseCtx, task, repo, referenceTypes, evaluator, concurrencyController, opts); err != nil {
				firstErr.Store(err)
				cancel()
				opts.endValidation()
				break LOOP
			}
			opts.endValidation()
		}
	}

	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return nil, nil, err.(error)
	}
	return task.subjectReport.ArtifactReports, evaluator, nil
}

// verifySubjectAgainstReferrers verifies the subject artifact against all
// referrers in the store and produces new tasks for each referrer.
func (e *Executor) verifySubjectAgainstReferrers(parentCtx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator, concurrencyController *concurrencyController, opts *validationOpts) error {
	artifact := task.artifact.String()
	var artifactReports []*ValidationReport
	var mu sync.Mutex

	addArtifactReport := func(report *ValidationReport) {
		mu.Lock()
		artifactReports = append(artifactReports, report)
		mu.Unlock()
	}

	err := e.Store.ListReferrers(parentCtx, artifact, referenceTypes, func(referrers []ocispec.Descriptor) error {
		ctx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		var firstErr atomic.Value
		var wg sync.WaitGroup

		for _, referrer := range referrers {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if concurrencyController.tryAcquire() {
				wg.Add(1)
				go func(referrer ocispec.Descriptor) {
					defer func() {
						concurrencyController.release()
						wg.Done()
					}()

					if err := e.processReferrer(ctx, task, repo, referenceTypes, evaluator, referrer, artifact, concurrencyController, opts, addArtifactReport); err != nil {
						firstErr.CompareAndSwap(nil, err)
						cancel()
					}
				}(referrer)
			} else {
				if err := e.processReferrer(ctx, task, repo, referenceTypes, evaluator, referrer, artifact, concurrencyController, opts, addArtifactReport); err != nil {
					firstErr.CompareAndSwap(nil, err)
					cancel()
				}
			}
		}
		wg.Wait()
		if err := firstErr.Load(); err != nil {
			return err.(error)
		}
		return nil
	})

	if err != nil {
		if err != errSubjectPruned {
			return fmt.Errorf("failed to verify referrers for artifact %s: %w", artifact, err)
		}
	}
	if evaluator != nil {
		if err := evaluator.Commit(parentCtx, task.artifactDesc.Digest.String()); err != nil {
			return fmt.Errorf("failed to commit the artifact %s: %w", artifact, err)
		}
	}
	task.subjectReport.ArtifactReports = append(task.subjectReport.ArtifactReports, artifactReports...)

	return nil
}

func (e *Executor) processReferrer(ctx context.Context, task *executorTask, repo string, referenceTypes []string, evaluator Evaluator, referrer ocispec.Descriptor, artifact string, concurrencyController *concurrencyController, opts *validationOpts, addArtifactReport func(*ValidationReport)) error {
	results, err := e.verifyArtifact(ctx, repo, task.artifactDesc, referrer, evaluator, concurrencyController)
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
			addArtifactReport(artifactReport)
		}
		return err
	}

	artifactReport := &ValidationReport{
		Subject:  artifact,
		Results:  results,
		Artifact: referrer,
	}
	addArtifactReport(artifactReport)

	referrerArtifact := task.artifact
	referrerArtifact.Reference = referrer.Digest.String()
	newTask := &executorTask{
		artifact:      referrerArtifact,
		artifactDesc:  referrer,
		subjectReport: artifactReport,
	}
	opts.taskQueue.push(newTask)
	return nil
}

// verifyArtifact verifies the artifact by all configured verifiers and returns
// error if any of the verifier fails.
func (e *Executor) verifyArtifact(parentCtx context.Context, repo string, subjectDesc, artifact ocispec.Descriptor, evaluator Evaluator, controller *concurrencyController) ([]*VerificationResult, error) {
	var verifierReports []*VerificationResult
	var mu sync.Mutex

	addVerifierReports := func(report *VerificationResult) {
		mu.Lock()
		defer mu.Unlock()
		verifierReports = append(verifierReports, report)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for _, verifier := range e.Verifiers {
		select {
		case <-ctx.Done():
			return verifierReports, ctx.Err()
		default:
		}

		if !verifier.Verifiable(artifact) {
			continue
		}

		if controller.tryAcquire() {
			wg.Add(1)
			go func(verifier Verifier) {
				defer func() {
					controller.release()
					wg.Done()
				}()

				err := e.processVerifier(ctx, subjectDesc, repo, artifact, verifier, evaluator, addVerifierReports)
				if err != nil {
					cancel()
					firstErr.CompareAndSwap(nil, err)
				}
			}(verifier)
		} else {
			err := e.processVerifier(ctx, subjectDesc, repo, artifact, verifier, evaluator, addVerifierReports)
			if err != nil {
				cancel()
				firstErr.CompareAndSwap(nil, err)
				break
			}
		}
	}

	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return verifierReports, err.(error)
	}

	return verifierReports, nil
}

func (e *Executor) processVerifier(ctx context.Context, subjectDesc ocispec.Descriptor, repo string, artifact ocispec.Descriptor, verifier Verifier, evaluator Evaluator, addVerifierReports func(report *VerificationResult)) error {
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
	addVerifierReports(verifierReport)
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

func validateExecutorSetup(store Store, verifiers []Verifier) error {
	if store == nil {
		return fmt.Errorf("store must be configured")
	}
	if len(verifiers) == 0 {
		return fmt.Errorf("at least one verifier must be configured")
	}
	return nil
}
