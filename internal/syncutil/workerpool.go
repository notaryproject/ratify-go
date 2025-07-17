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

package syncutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

var errPoolCompleted = errors.New("pool has already been completed")

type slot struct{}

// PoolSlots is a channel-based semaphore that limits the
// number of concurrent task in a [WorkerPool].
type PoolSlots chan slot

// WorkerPool is a worker pool that allows concurrent execution of tasks
type WorkerPool[Result any] struct {
	// eg is the errgroup that manages goroutines and error handling
	eg    *errgroup.Group
	egctx context.Context

	// poolSlots is a channel that limits the number of concurrent tasks
	poolSlots     PoolSlots
	dedicatedPool bool

	// results stores the results of completed tasks
	results   []Result
	resultsMu sync.Mutex

	// hasWaited is used to ensure Wait() can only be called once
	hasWaited atomic.Bool
}

// NewWorkerPool creates a worker pool with provided size.
//
// Result is the type of the results returned by the tasks in the pool.
// size is the number of concurrent tasks that can be executed in the pool.
// If size is less than or equal to 0, it defaults to 1.
func NewWorkerPool[Result any](ctx context.Context, size int) (*WorkerPool[Result], context.Context) {
	if size <= 0 {
		size = 1
	}
	pool, ctx := NewSharedWorkerPool[Result](ctx, make(PoolSlots, size))
	pool.dedicatedPool = true
	return pool, ctx
}

// NewSharedWorkerPool creates a worker pool that shares the provided pool slots.
//
// Result is the type of the results returned by the tasks in the pool.
func NewSharedWorkerPool[Result any](ctx context.Context, sharedSlots PoolSlots) (*WorkerPool[Result], context.Context) {
	eg, egCtx := errgroup.WithContext(ctx)
	return &WorkerPool[Result]{
		poolSlots: sharedSlots,
		eg:        eg,
		egctx:     egCtx,
	}, egCtx
}

// Go starts a concurrent task in the pool if a slot in the pool is
// available, or blocks until a slot becomes available.
//
// It returns an error if the pool has already been completed or if the context is done.
func (p *WorkerPool[Result]) Go(task func() (Result, error)) error {
	// check if Wait() has already been called first
	if p.hasWaited.Load() {
		return errPoolCompleted
	}

	// check cancellation
	select {
	case <-p.egctx.Done():
		if err := context.Cause(p.egctx); err != nil {
			return err
		}
		return p.egctx.Err()
	default:
	}

	select {
	case <-p.egctx.Done():
		if err := context.Cause(p.egctx); err != nil {
			return err
		}
		return p.egctx.Err()
	case p.poolSlots <- slot{}:
		// acquired a slot in the pool
	}

	p.eg.Go(func() error {
		defer func() {
			<-p.poolSlots // release pool slot
		}()

		// execute task
		result, err := task()

		// add result for both success and error cases
		p.resultsMu.Lock()
		p.results = append(p.results, result)
		p.resultsMu.Unlock()

		return err
	})

	return nil
}

// Wait blocks until all tasks in the pool have completed.
func (p *WorkerPool[Result]) Wait() ([]Result, error) {
	if !p.waitOnce() {
		return nil, errors.New("WorkerPool.Wait() can only be called once")
	}

	defer func() {
		if p.dedicatedPool {
			close(p.poolSlots)
		}
	}()

	// Wait for all goroutines to complete
	err := p.eg.Wait()
	return p.results, err
}

func (p *WorkerPool[Result]) waitOnce() bool {
	return p.hasWaited.CompareAndSwap(false, true)
}
