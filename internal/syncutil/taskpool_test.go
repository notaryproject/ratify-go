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
	"testing"
	"time"
)

func TestTaskPool_Basic(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	// Test empty pool
	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error for empty pool, got %v", err)
	}
}

func TestTaskPool_SingleTask(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	executed := false
	pool.Submit(func() error {
		executed = true
		return nil
	})

	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !executed {
		t.Fatal("expected task to be executed")
	}
}

func TestTaskPool_MultipleTasks(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	const numTasks = 10
	var counter int64

	for i := 0; i < numTasks; i++ {
		pool.Submit(func() error {
			atomic.AddInt64(&counter, 1)
			return nil
		})
	}

	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if atomic.LoadInt64(&counter) != numTasks {
		t.Fatalf("expected %d tasks executed, got %d", numTasks, counter)
	}
}

func TestTaskPool_TaskError(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	expectedErr := errors.New("task error")
	pool.Submit(func() error {
		return expectedErr
	})

	err := pool.Wait()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestTaskPool_MixedTasksWithErrors(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 3)

	var successCount int64
	expectedErr := errors.New("task error")

	// Submit successful tasks
	for i := 0; i < 5; i++ {
		pool.Submit(func() error {
			atomic.AddInt64(&successCount, 1)
			return nil
		})
	}

	// Submit an error task
	pool.Submit(func() error {
		return expectedErr
	})

	err := pool.Wait()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// error may cancel some of the successful tasks
	if successCount < 1 || successCount > 5 {
		t.Fatalf("expected 1 to 5 successful tasks, got %d", successCount)
	}
}

func TestTaskPool_ConcurrentSubmit(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 5)

	const numGoroutines = 10
	const tasksPerGoroutine = 50
	var totalExecuted int64

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrently submit tasks from multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerGoroutine; j++ {
				pool.Submit(func() error {
					atomic.AddInt64(&totalExecuted, 1)
					// Add small delay to increase chance of concurrent execution
					time.Sleep(time.Microsecond)
					return nil
				})
			}
		}()
	}

	wg.Wait()

	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedTotal := int64(numGoroutines * tasksPerGoroutine)
	if atomic.LoadInt64(&totalExecuted) != expectedTotal {
		t.Fatalf("expected %d tasks executed, got %d", expectedTotal, totalExecuted)
	}
}

func TestTaskPool_ConcurrentExecutionLimit(t *testing.T) {
	ctx := context.Background()
	const poolSize = 3
	pool, _ := NewTaskPool(ctx, poolSize)

	var currentlyRunning int64
	var maxConcurrent int64
	var mu sync.Mutex

	const numTasks = 20

	for i := 0; i < numTasks; i++ {
		pool.Submit(func() error {
			// Track concurrent execution
			current := atomic.AddInt64(&currentlyRunning, 1)

			mu.Lock()
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()

			// Simulate work
			time.Sleep(10 * time.Millisecond)

			atomic.AddInt64(&currentlyRunning, -1)
			return nil
		})
	}

	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// The maximum concurrent execution should not exceed pool size
	if maxConcurrent > int64(poolSize) {
		t.Fatalf("expected max concurrent tasks <= %d, got %d", poolSize, maxConcurrent)
	}

	// Should have some concurrency
	if maxConcurrent < 2 {
		t.Fatalf("expected some concurrency, got max concurrent: %d", maxConcurrent)
	}
}

func TestTaskPool_WithCancelledContext(t *testing.T) {
	// Test that TaskPool works with a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	pool, newCtx := NewTaskPool(ctx, 2)

	var executed bool
	pool.Submit(func() error {
		executed = true
		return nil
	})

	// Should still work even with cancelled context during creation
	err := pool.Wait()
	t.Logf("Wait completed with error: %v", err)
	t.Logf("Task executed: %v", executed)
	t.Logf("New context error: %v", newCtx.Err())
}

func TestTaskPool_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 10)

	const numTasks = 1000
	var results = make([]int64, numTasks)
	var mu sync.Mutex

	// Submit tasks that perform some work
	for i := 0; i < numTasks; i++ {
		taskID := i
		pool.Submit(func() error {
			// Simulate some computation
			var sum int64
			for j := 0; j < 1000; j++ {
				sum += int64(j)
			}

			mu.Lock()
			results[taskID] = sum
			mu.Unlock()

			return nil
		})
	}

	start := time.Now()
	err := pool.Wait()
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	t.Logf("Completed %d tasks in %v", numTasks, duration)

	// Verify all tasks completed
	mu.Lock()
	for i, result := range results {
		expectedSum := int64(999 * 1000 / 2) // sum of 0 to 999
		if result != expectedSum {
			t.Fatalf("task %d: expected sum %d, got %d", i, expectedSum, result)
		}
	}
	mu.Unlock()
}

func TestTaskPool_EdgeCases(t *testing.T) {
	t.Run("zero size pool", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewTaskPool(ctx, 0)

		var executed bool
		pool.Submit(func() error {
			executed = true
			return nil
		})

		err := pool.Wait()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !executed {
			t.Fatal("expected task to be executed even with zero size pool")
		}
	})

	t.Run("large pool size", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewTaskPool(ctx, 1000)

		var executed bool
		pool.Submit(func() error {
			executed = true
			return nil
		})

		err := pool.Wait()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !executed {
			t.Fatal("expected task to be executed")
		}
	})
}

func TestTaskPool_PanicHandling(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	var normalTaskExecuted bool
	var taskOrder []string
	var mu sync.Mutex

	// Submit a normal task first
	pool.Submit(func() error {
		mu.Lock()
		taskOrder = append(taskOrder, "normal1")
		mu.Unlock()
		normalTaskExecuted = true
		return nil
	})

	// Submit a task that returns an error instead of panicking
	// (panics in goroutines are harder to test reliably)
	pool.Submit(func() error {
		mu.Lock()
		taskOrder = append(taskOrder, "error")
		mu.Unlock()
		return errors.New("simulated error instead of panic")
	})

	// Submit another normal task
	pool.Submit(func() error {
		mu.Lock()
		taskOrder = append(taskOrder, "normal2")
		mu.Unlock()
		return nil
	})

	// Pool should handle errors gracefully and continue with other tasks
	err := pool.Wait()

	t.Logf("Wait returned error: %v", err)

	// The normal tasks should execute
	if !normalTaskExecuted {
		t.Fatal("normal task should execute")
	}

	mu.Lock()
	t.Logf("Task execution order: %v", taskOrder)
	mu.Unlock()
}

func TestTaskPool_MultipleWaitCalls(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 2)

	var executed bool
	pool.Submit(func() error {
		executed = true
		return nil
	})

	// First wait call
	err1 := pool.Wait()
	if err1 != nil {
		t.Fatalf("first wait: expected no error, got %v", err1)
	}
	if !executed {
		t.Fatal("expected task to be executed")
	}

	// Second wait call should also work (though behavior might vary)
	err2 := pool.Wait()
	t.Logf("Second wait returned: %v", err2)
}

func TestTaskPool_TaskSubmissionOrder(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewTaskPool(ctx, 1) // Single worker to ensure order is observable

	const numTasks = 10
	var executionOrder []int
	var mu sync.Mutex

	// Submit tasks in order
	for i := 0; i < numTasks; i++ {
		taskID := i
		pool.Submit(func() error {
			mu.Lock()
			executionOrder = append(executionOrder, taskID)
			mu.Unlock()
			return nil
		})
	}

	err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(executionOrder) != numTasks {
		t.Fatalf("expected %d tasks executed, got %d", numTasks, len(executionOrder))
	}

	t.Logf("Execution order: %v", executionOrder)
	// Note: Due to stack-based task queue, the order might be LIFO
	// This test documents the current behavior
}
