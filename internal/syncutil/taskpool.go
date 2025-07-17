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
	"sync"
)

// TaskPool is a pool that manages a stack of tasks and executes them concurrently
type TaskPool struct {
	wg         sync.WaitGroup
	tasks      *taskStack[func() error]
	workerpool *WorkerPool[any]
}

// NewTaskPool creates a new TaskPool with the specified size.
//
// size is the number of concurrent tasks that can be executed in the pool.
// If size is less than or equal to 0, it defaults to 1.
func NewTaskPool(ctx context.Context, size int) (*TaskPool, context.Context) {
	if size <= 0 {
		size = 1
	}
	pool, ctx := NewWorkerPool[any](ctx, size)
	p := &TaskPool{
		wg:         sync.WaitGroup{},
		tasks:      NewTaskStack[func() error](size),
		workerpool: pool,
	}

	// Start a goroutine to process tasks from the stack
	go func() {
		for task := range p.tasks.Channel() {
			if err := pool.Go(func() (any, error) {
				defer p.wg.Done()
				return nil, task()
			}); err != nil {
				p.wg.Done()
				// error will be handled by Wait()
			}
		}
	}()

	return p, ctx
}

// Submit adds a task to the TaskPool for execution.
func (p *TaskPool) Submit(task func() error) {
	p.wg.Add(1)
	p.tasks.Push(task)
}

// Wait waits for all tasks in the TaskPool to complete and returns any errors encountered during execution.
func (p *TaskPool) Wait() error {
	defer p.tasks.Close()

	p.wg.Wait()
	if _, err := p.workerpool.Wait(); err != nil {
		return err
	}
	return nil
}
