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
	"testing"
	"time"
)

func TestWorkerPool_Basic(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	// test empty group
	results, err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty results, got %v", results)
	}
}

func TestWorkerPool_SingleTask(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	err := pool.Go(func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	results, err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(results) != 1 || results[0] != 42 {
		t.Fatalf("expected [42], got %v", results)
	}
}

func TestWorkerPool_MultipleTasks(t *testing.T) {
	poolSlots := make(PoolSlots, 3)
	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	for i := 1; i <= 5; i++ {
		i := i // capture loop variable
		err := pool.Go(func() (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("failed to submit task %d: %v", i, err)
		}
	}

	results, err := pool.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestWorkerPool_WithError(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	expectedErr := errors.New("task error")

	err := pool.Go(func() (int, error) {
		return 0, expectedErr
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	err = pool.Go(func() (int, error) {
		time.Sleep(100 * time.Millisecond) // simulate work
		return 42, nil
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	_, err = pool.Wait()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	if err.Error() != expectedErr.Error() {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestWorkerPool_WaitCalledTwice(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	// first call should succeed
	_, err := pool.Wait()
	if err != nil {
		t.Fatalf("first Wait() call failed: %v", err)
	}

	// second call should fail
	_, err = pool.Wait()
	if err == nil {
		t.Fatal("expected error on second Wait() call, got nil")
	}
}

func TestWorkerPool_PanicRecovery(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	// add a task that panics
	err := pool.Go(func() (int, error) {
		panic("test panic")
	})
	if err != nil {
		t.Fatalf("unexpected error adding task: %v", err)
	}

	// add a normal task
	err = pool.Go(func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error adding task: %v", err)
	}

	// wait should re-raise the panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to be re-raised, but no panic occurred")
		}
	}()

	_, _ = pool.Wait()
	t.Fatal("Wait() should have panicked but returned normally")
}

func TestWorkerPool_GoAfterWait(t *testing.T) {
	poolSlots := make(PoolSlots, 2)

	ctx := context.Background()
	pool, _ := NewSharedWorkerPool[int](ctx, poolSlots)

	// Add a task to the pool
	err := pool.Go(func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	// Wait for all tasks to complete
	results, err := pool.Wait()
	if err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	if len(results) != 1 || results[0] != 42 {
		t.Fatalf("expected [42], got %v", results)
	}

	// Try to add another task after Wait() has been called
	err = pool.Go(func() (int, error) {
		return 100, nil
	})
	if err == nil {
		t.Fatal("expected error when calling Go() after Wait(), got nil")
	}

	expectedErrMsg := "pool has already been completed"
	if err.Error() != expectedErrMsg {
		t.Fatalf("expected error message %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestWorkerPool_DedicatedPoolClosesSlots(t *testing.T) {
	ctx := context.Background()
	pool, _ := NewWorkerPool[int](ctx, 2) // Create a dedicated pool with size 2

	// Add some tasks to the pool
	err := pool.Go(func() (int, error) {
		time.Sleep(10 * time.Millisecond) // simulate some work
		return 1, nil
	})
	if err != nil {
		t.Fatalf("failed to submit first task: %v", err)
	}

	err = pool.Go(func() (int, error) {
		time.Sleep(10 * time.Millisecond) // simulate some work
		return 2, nil
	})
	if err != nil {
		t.Fatalf("failed to submit second task: %v", err)
	}

	// Wait for all tasks to complete
	results, err := pool.Wait()
	if err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Verify that the poolSlots channel is closed
	// Since it's a dedicated pool, the channel should be closed after Wait()
	select {
	case _, ok := <-pool.poolSlots:
		if ok {
			t.Fatal("expected poolSlots channel to be closed, but it's still open")
		}
		// Channel is closed as expected
	default:
		t.Fatal("expected poolSlots channel to be closed and readable, but it's not")
	}
}

func TestWorkerPool_Cancellation(t *testing.T) {
	t.Run("parent cancel child", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create parent worker pool
		parentPool, ctxParent := NewWorkerPool[int](ctx, 1)

		// Create child worker pool using parent's context
		childPool, ctxChild := NewWorkerPool[int](ctxParent, 1)

		// Channels to track task execution
		parentTaskStarted := make(chan bool)
		childTaskStarted := make(chan bool)
		parentTaskCancelled := make(chan bool)
		childTaskCancelled := make(chan bool)

		// Add task to parent pool
		err := parentPool.Go(func() (int, error) {
			parentTaskStarted <- true
			select {
			case <-ctxParent.Done():
				parentTaskCancelled <- true
				return 0, ctxParent.Err()
			case <-time.After(5 * time.Second):
				return 1, nil
			}
		})
		if err != nil {
			t.Fatalf("failed to submit parent task: %v", err)
		}

		// Add task to child pool
		err = childPool.Go(func() (int, error) {
			childTaskStarted <- true
			select {
			case <-ctxChild.Done():
				childTaskCancelled <- true
				return 0, ctxChild.Err()
			case <-time.After(5 * time.Second):
				return 2, nil
			}
		})
		if err != nil {
			t.Fatalf("failed to submit child task: %v", err)
		}

		// Wait for both tasks to start
		<-parentTaskStarted
		<-childTaskStarted

		// Cancel the root context (parent of parent pool)
		cancel()

		// Both parent and child tasks should be cancelled
		select {
		case <-parentTaskCancelled:
		case <-time.After(1 * time.Second):
			t.Fatal("parent task was not cancelled within timeout")
		}

		select {
		case <-childTaskCancelled:
		case <-time.After(1 * time.Second):
			t.Fatal("child task was not cancelled within timeout")
		}

		// Both pools should return cancellation errors
		_, err = parentPool.Wait()
		if err == nil {
			t.Fatal("expected cancellation error from parent pool, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled error from parent pool, got: %v", err)
		}

		_, err = childPool.Wait()
		if err == nil {
			t.Fatal("expected cancellation error from child pool, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled error from child pool, got: %v", err)
		}
	})

	t.Run("child cancel does not affect parent", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create parent worker pool
		parentPool, ctxParent := NewWorkerPool[int](ctx, 1)

		// Create child worker pool using parent's context
		childPool, _ := NewWorkerPool[int](ctxParent, 1)

		// Channels to track task execution
		parentTaskStarted := make(chan bool)
		childTaskStarted := make(chan bool)
		parentTaskCompleted := make(chan bool)

		// Add task to parent pool that should complete successfully
		err := parentPool.Go(func() (int, error) {
			parentTaskStarted <- true
			// Parent task does work and completes normally
			time.Sleep(200 * time.Millisecond)
			parentTaskCompleted <- true
			return 1, nil
		})
		if err != nil {
			t.Fatalf("failed to submit parent task: %v", err)
		}

		// Add task to child pool that will encounter an error (simulating child cancellation)
		err = childPool.Go(func() (int, error) {
			childTaskStarted <- true
			// Child task fails quickly
			time.Sleep(50 * time.Millisecond)
			return 0, errors.New("child task failed")
		})
		if err != nil {
			t.Fatalf("failed to submit child task: %v", err)
		}

		// Wait for both tasks to start
		<-parentTaskStarted
		<-childTaskStarted

		// Parent task should complete successfully despite child failure
		select {
		case <-parentTaskCompleted:
		case <-time.After(1 * time.Second):
			t.Fatal("parent task did not complete within timeout")
		}

		// Parent context should still be active
		select {
		case <-ctxParent.Done():
			t.Fatal("parent context should not be cancelled when child fails")
		default:
			// Good, parent context is still active
		}

		// Parent pool should complete successfully
		parentResults, err := parentPool.Wait()
		if err != nil {
			t.Fatalf("parent pool should not have error, got: %v", err)
		}
		if len(parentResults) != 1 || parentResults[0] != 1 {
			t.Fatalf("expected parent result [1], got %v", parentResults)
		}

		// Child pool should have the error
		_, err = childPool.Wait()
		if err == nil {
			t.Fatal("expected error from child pool, got nil")
		}
		expectedErrMsg := "child task failed"
		if err.Error() != expectedErrMsg {
			t.Fatalf("expected error message %q, got %q", expectedErrMsg, err.Error())
		}
	})

	t.Run("multiple children with independent cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create parent worker pool
		parentPool, ctxParent := NewWorkerPool[int](ctx, 1)

		// Create two child worker pools
		child1Pool, _ := NewWorkerPool[int](ctxParent, 1)
		child2Pool, _ := NewWorkerPool[int](ctxParent, 1)

		// Channels to track execution
		parentTaskCompleted := make(chan bool)
		child1TaskStarted := make(chan bool)
		child2TaskCompleted := make(chan bool)

		// Parent task that should complete
		err := parentPool.Go(func() (int, error) {
			time.Sleep(150 * time.Millisecond)
			parentTaskCompleted <- true
			return 1, nil
		})
		if err != nil {
			t.Fatalf("failed to submit parent task: %v", err)
		}

		// Child1 task that will fail
		err = child1Pool.Go(func() (int, error) {
			child1TaskStarted <- true
			time.Sleep(50 * time.Millisecond)
			return 0, errors.New("child1 failed")
		})
		if err != nil {
			t.Fatalf("failed to submit child1 task: %v", err)
		}

		// Child2 task that should complete
		err = child2Pool.Go(func() (int, error) {
			time.Sleep(100 * time.Millisecond)
			child2TaskCompleted <- true
			return 2, nil
		})
		if err != nil {
			t.Fatalf("failed to submit child2 task: %v", err)
		}

		// Wait for child1 to start
		<-child1TaskStarted

		// Wait for parent and child2 to complete
		<-parentTaskCompleted
		<-child2TaskCompleted

		// Parent should complete successfully
		parentResults, err := parentPool.Wait()
		if err != nil {
			t.Fatalf("parent pool should not have error, got: %v", err)
		}
		if len(parentResults) != 1 || parentResults[0] != 1 {
			t.Fatalf("expected parent result [1], got %v", parentResults)
		}

		// Child1 should have error
		_, err = child1Pool.Wait()
		if err == nil {
			t.Fatal("expected error from child1 pool, got nil")
		}

		// Child2 should complete successfully
		child2Results, err := child2Pool.Wait()
		if err != nil {
			t.Fatalf("child2 pool should not have error, got: %v", err)
		}
		if len(child2Results) != 1 || child2Results[0] != 2 {
			t.Fatalf("expected child2 result [2], got %v", child2Results)
		}
	})
}
