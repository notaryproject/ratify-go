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

package sync

import (
	"sync"
	"testing"
	"time"
)

func TestStack_NewStack(t *testing.T) {
	stack := NewStack[int]()
	if stack == nil {
		t.Error("Expected NewStack to return a non-nil stack")
	}
	if stack.closed {
		t.Error("Expected new stack to not be closed")
	}
	if stack.activeWorkers != 0 {
		t.Error("Expected new stack to have 0 active workers")
	}
}

func TestStack_Push(t *testing.T) {
	stack := NewStack[int]()

	// Test pushing to empty stack
	stack.Push(1)
	if len(stack.items) != 1 {
		t.Errorf("Expected stack length to be 1, got %d", len(stack.items))
	}

	// Test pushing multiple items
	stack.Push(2)
	stack.Push(3)
	if len(stack.items) != 3 {
		t.Errorf("Expected stack length to be 3, got %d", len(stack.items))
	}
}

func TestStack_Pop(t *testing.T) {
	stack := NewStack[int]()

	// Push some items
	stack.Push(1)
	stack.Push(2)
	stack.Push(3)

	// Pop items and verify LIFO order
	item, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	if item != 3 {
		t.Errorf("Expected popped item to be 3, got %d", item)
	}
	if stack.activeWorkers != 1 {
		t.Errorf("Expected activeWorkers to be 1, got %d", stack.activeWorkers)
	}

	item, ok = stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	if item != 2 {
		t.Errorf("Expected popped item to be 2, got %d", item)
	}
	if stack.activeWorkers != 2 {
		t.Errorf("Expected activeWorkers to be 2, got %d", stack.activeWorkers)
	}
}

func TestStack_Done(t *testing.T) {
	stack := NewStack[int]()

	// Push and pop an item
	stack.Push(1)
	_, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}

	// Call Done to decrease active workers
	stack.Done()
	if stack.activeWorkers != 0 {
		t.Errorf("Expected activeWorkers to be 0, got %d", stack.activeWorkers)
	}
}

func TestStack_PopBlocking(t *testing.T) {
	stack := NewStack[int]()

	// Channel to coordinate goroutines
	started := make(chan bool)
	finished := make(chan bool)

	// Start a goroutine that will block on Pop
	go func() {
		started <- true
		item, ok := stack.Pop()
		if !ok {
			t.Error("Expected Pop to return true")
			return
		}
		if item != 42 {
			t.Errorf("Expected popped item to be 42, got %d", item)
		}
		finished <- true
	}()

	// Wait for goroutine to start
	<-started

	// Give some time to ensure Pop is blocking
	time.Sleep(10 * time.Millisecond)

	// Push an item to unblock Pop
	stack.Push(42)

	// Wait for goroutine to finish
	select {
	case <-finished:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Pop did not unblock after Push")
	}
}

func TestStack_ConcurrentPushPop(t *testing.T) {
	stack := NewStack[int]()

	const numItems = 100

	var wg sync.WaitGroup

	// Start a producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			stack.Push(i)
		}
	}()

	// Start a consumer
	consumedItems := make([]int, 0, numItems)
	var itemsMu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			item, ok := stack.Pop()
			if !ok {
				t.Error("Expected Pop to return true")
				return
			}
			itemsMu.Lock()
			consumedItems = append(consumedItems, item)
			itemsMu.Unlock()
		}
	}()

	wg.Wait()

	// Verify all items were consumed
	if len(consumedItems) != numItems {
		t.Errorf("Expected %d items, got %d", numItems, len(consumedItems))
	}
}

func TestStack_AutoClose(t *testing.T) {
	stack := NewStack[int]()

	// Push an item
	stack.Push(1)

	// Pop the item
	item, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	if item != 1 {
		t.Errorf("Expected popped item to be 1, got %d", item)
	}

	// Call Done - this should close the stack since no items left and no active workers
	stack.Done()

	if !stack.closed {
		t.Error("Expected stack to be closed after Done() with no items and no active workers")
	}
}

func TestStack_PopFromClosedStack(t *testing.T) {
	stack := NewStack[int]()

	// Close the stack by pushing, popping, and calling Done
	stack.Push(1)
	_, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	stack.Done()

	// Try to pop from closed stack
	_, ok = stack.Pop()
	if ok {
		t.Error("Expected Pop from closed stack to return false")
	}
}

func TestStack_PushToClosedStack(t *testing.T) {
	stack := NewStack[int]()

	// Close the stack
	stack.Push(1)
	_, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	stack.Done()

	initialLen := len(stack.items)

	// Try to push to closed stack
	stack.Push(2)

	// Verify nothing was added
	if len(stack.items) != initialLen {
		t.Error("Expected Push to closed stack to be ignored")
	}
}

func TestStack_MultipleWorkersAutoClose(t *testing.T) {
	stack := NewStack[int]()

	// Push some items
	for i := 0; i < 3; i++ {
		stack.Push(i)
	}

	// Pop all items but don't call Done yet
	for i := 0; i < 3; i++ {
		_, ok := stack.Pop()
		if !ok {
			t.Error("Expected Pop to return true")
		}
	}

	// Stack should not be closed yet (activeWorkers > 0)
	if stack.closed {
		t.Error("Expected stack to not be closed while workers are active")
	}

	// Call Done for all but one worker
	for i := 0; i < 2; i++ {
		stack.Done()
	}

	// Stack should still not be closed
	if stack.closed {
		t.Error("Expected stack to not be closed while workers are still active")
	}

	// Call Done for the last worker
	stack.Done()

	// Now stack should be closed
	if !stack.closed {
		t.Error("Expected stack to be closed after all workers are done")
	}
}

func TestStack_WaitForItemsMultipleConsumers(t *testing.T) {
	stack := NewStack[int]()

	const numConsumers = 5
	results := make(chan int, numConsumers)

	// Start multiple consumers that will block
	for i := 0; i < numConsumers; i++ {
		go func() {
			item, ok := stack.Pop()
			if ok {
				results <- item
			} else {
				results <- -1 // Indicate failure
			}
		}()
	}

	// Give consumers time to start and block
	time.Sleep(10 * time.Millisecond)

	// Push items to unblock consumers
	for i := 0; i < numConsumers; i++ {
		stack.Push(i)
	}

	// Collect results
	received := make([]int, 0, numConsumers)
	for i := 0; i < numConsumers; i++ {
		select {
		case result := <-results:
			if result == -1 {
				t.Error("Consumer failed to get item")
			} else {
				received = append(received, result)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Consumer did not receive item in time")
		}
	}

	if len(received) != numConsumers {
		t.Errorf("Expected %d results, got %d", numConsumers, len(received))
	}
}

func TestStack_ZeroValue(t *testing.T) {
	stack := NewStack[string]()

	// Push empty string (zero value for string)
	stack.Push("")

	item, ok := stack.Pop()
	if !ok {
		t.Error("Expected Pop to return true")
	}
	if item != "" {
		t.Errorf("Expected empty string, got %q", item)
	}
}
