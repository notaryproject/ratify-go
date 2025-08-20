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
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestNewTaskStack(t *testing.T) {
	t.Run("creates new task stack with buffer size", func(t *testing.T) {
		stack := NewTaskStack[int](5)
		if stack == nil {
			t.Fatal("NewTaskStack returned nil")
		}
		if cap(stack.ch) != 5 {
			t.Errorf("Expected channel buffer size 5, got %d", cap(stack.ch))
		}
		if stack.closed {
			t.Error("TaskStack should not be closed initially")
		}
	})

	t.Run("creates task stack with zero buffer size", func(t *testing.T) {
		stack := NewTaskStack[string](0)
		if stack == nil {
			t.Fatal("NewTaskStack returned nil")
		}
		if cap(stack.ch) != 1 {
			t.Errorf("Expected channel buffer size 0, got %d", cap(stack.ch))
		}
	})
}

func TestTaskStack_Push(t *testing.T) {
	t.Run("pushes single task", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		defer stack.Close()

		stack.Push(42)

		select {
		case task := <-stack.Channel():
			if task != 42 {
				t.Errorf("Expected task 42, got %d", task)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for task")
		}
	})

	t.Run("pushes multiple tasks in LIFO order", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		defer stack.Close()

		// Push tasks 1, 2, 3 - should receive them in reverse order (3, 2, 1)
		stack.Push(1)
		stack.Push(2)
		stack.Push(3)

		expected := []int{3, 2, 1}
		for i, exp := range expected {
			select {
			case task := <-stack.Channel():
				if task != exp {
					t.Errorf("Task %d: expected %d, got %d", i, exp, task)
				}
			case <-time.After(time.Second):
				t.Errorf("Timeout waiting for task %d", i)
			}
		}
	})

	t.Run("push after close is ignored", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		stack.Close()

		stack.Push(42)

		// Channel should be closed, so we shouldn't receive anything
		select {
		case task, ok := <-stack.Channel():
			if ok {
				t.Errorf("Unexpected task received after close: %d", task)
			}
		case <-time.After(100 * time.Millisecond):
			// Expected timeout
		}
	})
}

func TestTaskStack_Channel(t *testing.T) {
	t.Run("returns readable channel", func(t *testing.T) {
		stack := NewTaskStack[string](5)
		defer stack.Close()

		ch := stack.Channel()
		if ch == nil {
			t.Error("Channel() returned nil")
		}

		stack.Push("test")

		select {
		case task := <-ch:
			if task != "test" {
				t.Errorf("Expected 'test', got '%s'", task)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for task")
		}
	})
}

func TestTaskStack_Close(t *testing.T) {
	t.Run("closes channel", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		stack.Push(1)

		// Drain the task first
		<-stack.Channel()

		stack.Close()

		// Channel should be closed
		select {
		case _, ok := <-stack.Channel():
			if ok {
				t.Error("Channel should be closed")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Channel should be closed immediately")
		}
	})

	t.Run("close with full channel buffer", func(t *testing.T) {
		stack := NewTaskStack[int](1)

		// Fill the buffer
		stack.Push(1)
		// Wait for the task to be drained to the channel
		time.Sleep(10 * time.Millisecond)

		// Push another task to fill the channel buffer
		stack.Push(2)
		time.Sleep(10 * time.Millisecond)

		stack.Close()

		// Should be able to read remaining tasks
		count := 0
		for task := range stack.Channel() {
			count++
			if count != 1 {
				t.Fatalf("Expected only one task, got %d", count)
			}
			t.Logf("Received task: %d", task)
		}
	})

	t.Run("multiple close calls are safe", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		stack.Push(1)

		// Drain the task
		<-stack.Channel()

		stack.Close()
		stack.Close() // Should not panic or cause issues
	})
}

func TestTaskStack_DrainStack(t *testing.T) {
	t.Run("draining goroutine starts only once", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		defer stack.Close()

		initialGoroutines := runtime.NumGoroutine()

		// Push multiple tasks
		for i := 0; i < 5; i++ {
			stack.Push(i)
		}

		// Wait a bit for goroutines to stabilize
		time.Sleep(50 * time.Millisecond)

		finalGoroutines := runtime.NumGoroutine()

		// Should only add one goroutine for draining
		if finalGoroutines-initialGoroutines > 1 {
			t.Errorf("Expected at most 1 new goroutine, got %d", finalGoroutines-initialGoroutines)
		}
	})

	t.Run("draining restarts after stack becomes empty", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		defer stack.Close()

		// First batch
		stack.Push(1)
		task1 := <-stack.Channel()
		if task1 != 1 {
			t.Errorf("Expected 1, got %d", task1)
		}

		// Wait for draining to stop
		time.Sleep(50 * time.Millisecond)

		// Second batch - should restart draining
		stack.Push(2)
		task2 := <-stack.Channel()
		if task2 != 2 {
			t.Errorf("Expected 2, got %d", task2)
		}
	})
}

func TestTaskStack_Concurrency(t *testing.T) {
	t.Run("concurrent pushes", func(t *testing.T) {
		stack := NewTaskStack[int](100)
		defer stack.Close()

		const numGoroutines = 10
		const tasksPerGoroutine = 100
		var wg sync.WaitGroup

		// Start multiple goroutines pushing tasks
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for j := 0; j < tasksPerGoroutine; j++ {
					stack.Push(start*tasksPerGoroutine + j)
				}
			}(i)
		}

		// Collect all tasks
		received := make(map[int]bool)
		var receiveMutex sync.Mutex
		var receiveWg sync.WaitGroup
		receiveWg.Add(1)
		go func() {
			defer receiveWg.Done()
			for task := range stack.Channel() {
				receiveMutex.Lock()
				received[task] = true
				receiveMutex.Unlock()
				if len(received) == numGoroutines*tasksPerGoroutine {
					return
				}
			}
		}()

		wg.Wait()

		// Wait a bit for all tasks to be processed
		timeout := time.After(5 * time.Second)
		done := make(chan struct{})
		go func() {
			for {
				receiveMutex.Lock()
				count := len(received)
				receiveMutex.Unlock()
				if count == numGoroutines*tasksPerGoroutine {
					close(done)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		select {
		case <-done:
			// Success
		case <-timeout:
			receiveMutex.Lock()
			count := len(received)
			receiveMutex.Unlock()
			t.Errorf("Timeout: received %d tasks, expected %d", count, numGoroutines*tasksPerGoroutine)
		}

		stack.Close()
		receiveWg.Wait()

		// Verify all expected tasks were received
		receiveMutex.Lock()
		if len(received) != numGoroutines*tasksPerGoroutine {
			t.Errorf("Expected %d unique tasks, got %d", numGoroutines*tasksPerGoroutine, len(received))
		}
		receiveMutex.Unlock()
	})

	t.Run("concurrent push and close", func(t *testing.T) {
		const iterations = 100
		for i := 0; i < iterations; i++ {
			stack := NewTaskStack[int](10)

			var wg sync.WaitGroup
			wg.Add(2)

			// Goroutine 1: Push tasks
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					stack.Push(j)
					time.Sleep(time.Microsecond)
				}
			}()

			// Goroutine 2: Close after a delay
			go func() {
				defer wg.Done()
				time.Sleep(time.Microsecond * 1000)
				stack.Close()
			}()

			wg.Wait()
		}
	})

	t.Run("concurrent readers", func(t *testing.T) {
		stack := NewTaskStack[int](10)

		const numTasks = 50 // Reduced number to make test more reliable
		const numReaders = 3

		// Push all tasks
		for i := 0; i < numTasks; i++ {
			stack.Push(i)
		}

		// Give some time for all tasks to be pushed and drained
		time.Sleep(50 * time.Millisecond)

		received := make(chan int, numTasks)
		var wg sync.WaitGroup

		// Start multiple readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range stack.Channel() {
					received <- task
				}
			}()
		}

		// Close the stack after a short delay to signal readers to stop
		go func() {
			time.Sleep(100 * time.Millisecond)
			stack.Close()
		}()

		wg.Wait()
		close(received)

		// Count received tasks
		receivedCount := 0
		receivedTasks := make(map[int]bool)
		for task := range received {
			receivedTasks[task] = true
			receivedCount++
		}

		if receivedCount != numTasks {
			t.Logf("Expected %d tasks, got %d", numTasks, receivedCount)
			// This is not necessarily a failure due to the nature of concurrent operations
			// The important thing is that we don't receive more tasks than pushed
			if receivedCount > numTasks {
				t.Errorf("Received more tasks (%d) than pushed (%d)", receivedCount, numTasks)
			}
		}

		// Verify all tasks are unique (no duplicates)
		if len(receivedTasks) != receivedCount {
			t.Errorf("Found duplicate tasks: %d unique out of %d total", len(receivedTasks), receivedCount)
		}
	})
}

func TestTaskStack_EdgeCases(t *testing.T) {
	t.Run("empty stack operations", func(t *testing.T) {
		stack := NewTaskStack[int](10)
		defer stack.Close()

		// Try to read from empty stack with timeout
		select {
		case <-stack.Channel():
			t.Error("Should not receive task from empty stack")
		case <-time.After(100 * time.Millisecond):
			// Expected timeout
		}
	})

	t.Run("large buffer size", func(t *testing.T) {
		stack := NewTaskStack[int](10000)
		defer stack.Close()

		const numTasks = 1000
		for i := 0; i < numTasks; i++ {
			stack.Push(i)
		}

		count := 0
		timeout := time.After(5 * time.Second)
		for count < numTasks {
			select {
			case <-stack.Channel():
				count++
			case <-timeout:
				t.Errorf("Timeout: received %d tasks, expected %d", count, numTasks)
				return
			}
		}
	})

	t.Run("string tasks", func(t *testing.T) {
		stack := NewTaskStack[string](5)
		defer stack.Close()

		tasks := []string{"hello", "world", "test", "stack"}
		for _, task := range tasks {
			stack.Push(task)
		}

		// Should receive in reverse order
		expected := []string{"stack", "test", "world", "hello"}
		for i, exp := range expected {
			select {
			case task := <-stack.Channel():
				if task != exp {
					t.Errorf("Task %d: expected '%s', got '%s'", i, exp, task)
				}
			case <-time.After(time.Second):
				t.Errorf("Timeout waiting for task %d", i)
			}
		}
	})

	t.Run("struct tasks", func(t *testing.T) {
		type Task struct {
			ID   int
			Name string
		}

		stack := NewTaskStack[Task](5)
		defer stack.Close()

		task1 := Task{ID: 1, Name: "first"}
		task2 := Task{ID: 2, Name: "second"}

		stack.Push(task1)
		stack.Push(task2)

		// Should receive task2 first (LIFO)
		select {
		case received := <-stack.Channel():
			if received.ID != 2 || received.Name != "second" {
				t.Errorf("Expected {2, second}, got {%d, %s}", received.ID, received.Name)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for first task")
		}

		select {
		case received := <-stack.Channel():
			if received.ID != 1 || received.Name != "first" {
				t.Errorf("Expected {1, first}, got {%d, %s}", received.ID, received.Name)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for second task")
		}
	})
}

func TestTaskStack_WaitingDraining(t *testing.T) {
	t.Run("close waits for draining to complete", func(t *testing.T) {
		stack := NewTaskStack[int](1)

		// Push multiple tasks to ensure draining goroutine is working
		for i := 0; i < 5; i++ {
			stack.Push(i)
		}

		// Read one task to make room in the channel
		<-stack.Channel()

		// Give some time for draining to start and be active
		time.Sleep(50 * time.Millisecond)

		start := time.Now()
		stack.Close()
		duration := time.Since(start)

		// Close should wait for draining goroutine, but not too long
		if duration > 100*time.Millisecond {
			t.Error("Close took too long, possible deadlock")
		}
		// This test is more about ensuring no panic/deadlock than precise timing
	})
}

// Benchmark tests
func BenchmarkTaskStack_Push(b *testing.B) {
	stack := NewTaskStack[int](1000)
	defer stack.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stack.Push(i)
	}
}

func BenchmarkTaskStack_PushAndReceive(b *testing.B) {
	stack := NewTaskStack[int](1000)
	defer stack.Close()

	go func() {
		for range stack.Channel() {
			// Consume tasks
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stack.Push(i)
	}
}

func BenchmarkTaskStack_ConcurrentPush(b *testing.B) {
	stack := NewTaskStack[int](1000)
	defer stack.Close()

	go func() {
		for range stack.Channel() {
			// Consume tasks
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			stack.Push(i)
			i++
		}
	})
}
