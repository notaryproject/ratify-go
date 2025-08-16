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

func TestStackPushPop(t *testing.T) {
	stack := NewStack[string]()

	// Test that pop blocks when stack is empty
	popCh := stack.Pop()

	// Push an item
	go func() {
		time.Sleep(10 * time.Millisecond) // Small delay
		stack.Push("hello")
	}()

	// Pop should receive the item
	select {
	case item := <-popCh:
		if item != "hello" {
			t.Errorf("Expected 'hello', got '%s'", item)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Pop operation timed out")
	}
}

func TestStackLIFO(t *testing.T) {
	stack := NewStack[string]()

	// Start two pop operations
	pop1Ch := stack.Pop()
	pop2Ch := stack.Pop()

	// Push items - they should be received in LIFO order
	go func() {
		time.Sleep(10 * time.Millisecond)
		stack.Push("first")
		time.Sleep(10 * time.Millisecond)
		stack.Push("second")
	}()

	// The most recent pop (pop2) should get the first push
	// The earlier pop (pop1) should get the second push
	var item1, item2 string
	var received1, received2 bool

	for i := 0; i < 2; i++ {
		select {
		case item := <-pop1Ch:
			item1 = item
			received1 = true
		case item := <-pop2Ch:
			item2 = item
			received2 = true
		case <-time.After(200 * time.Millisecond):
			t.Error("Pop operation timed out")
		}
	}

	if !received1 || !received2 {
		t.Error("Not all pop operations completed")
	}

	// pop2 (more recent) should get "first", pop1 should get "second"
	if item2 != "first" {
		t.Errorf("Expected pop2 to get 'first', got '%s'", item2)
	}
	if item1 != "second" {
		t.Errorf("Expected pop1 to get 'second', got '%s'", item1)
	}
}

func TestStackConcurrency(t *testing.T) {
	stack := NewStack[int]()
	const numGoroutines = 10
	const itemsPerGoroutine = 100

	var wg sync.WaitGroup

	// Start pop operations
	results := make([]chan int, numGoroutines*itemsPerGoroutine)
	for i := 0; i < numGoroutines*itemsPerGoroutine; i++ {
		results[i] = make(chan int, 1)
		go func(index int) {
			popCh := stack.Pop()
			select {
			case item := <-popCh:
				results[index] <- item
			case <-time.After(time.Second):
				t.Errorf("Pop operation %d timed out", index)
				results[index] <- -1
			}
		}(i)
	}

	// Push items
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				stack.Push(start*itemsPerGoroutine + j)
			}
		}(i)
	}

	wg.Wait()

	// Collect all results
	received := make(map[int]bool)
	for i := 0; i < numGoroutines*itemsPerGoroutine; i++ {
		select {
		case item := <-results[i]:
			if item >= 0 {
				received[item] = true
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Result %d not received", i)
		}
	}

	// Verify all items were received
	expectedCount := numGoroutines * itemsPerGoroutine
	if len(received) != expectedCount {
		t.Errorf("Expected %d unique items, got %d", expectedCount, len(received))
	}
}

func TestStackPushNonBlocking(t *testing.T) {
	stack := NewStack[int]()

	// Test that push is non-blocking even when there are no waiters
	done := make(chan bool, 1)

	go func() {
		// These pushes should complete immediately even with no waiters
		for i := 0; i < 1000; i++ {
			stack.Push(i)
		}
		done <- true
	}()

	// Push operations should complete quickly
	select {
	case <-done:
		// Success - push operations completed without blocking
	case <-time.After(100 * time.Millisecond):
		t.Error("Push operations appeared to block, but they should be non-blocking")
	}

	// Since there were no waiters, items should be stored in the stack
	// Verify by checking that a subsequent pop operation returns the last item (LIFO)
	popCh := stack.Pop()
	select {
	case item := <-popCh:
		// Should get the last pushed item (999) due to LIFO behavior
		if item != 999 {
			t.Errorf("Expected to get the last pushed item (999), but got: %v", item)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Pop operation timed out, but items should be available")
	}
}

func TestStackIsEmpty(t *testing.T) {
	stack := NewStack[int]()

	// IsEmpty should always return true in this implementation
	if !stack.IsEmpty() {
		t.Error("Expected stack to be empty")
	}
}

func TestStackClose(t *testing.T) {
	stack := NewStack[int]()

	// Start some pop operations
	pop1 := stack.Pop()
	pop2 := stack.Pop()

	// Close the stack
	stack.Close()

	// Channels should be closed
	select {
	case _, ok := <-pop1:
		if ok {
			t.Error("Expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Channel close not detected")
	}

	select {
	case _, ok := <-pop2:
		if ok {
			t.Error("Expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Channel close not detected")
	}
}

// Benchmark tests
func BenchmarkStackPush(b *testing.B) {
	stack := NewStack[int]()

	// Start background pop operations to consume items
	go func() {
		for {
			<-stack.Pop()
		}
	}()

	// Small delay to ensure pop operation is ready
	time.Sleep(time.Millisecond)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			stack.Push(i)
			i++
		}
	})
}

func BenchmarkStackPop(b *testing.B) {
	stack := NewStack[int]()

	// Start background push operations to provide items
	go func() {
		i := 0
		for {
			stack.Push(i)
			i++
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			<-stack.Pop()
		}
	})
}

// TestStackNoItemsDropped verifies that push operations don't drop items
// when there are no waiters, and that all items can be retrieved later.
func TestStackNoItemsDropped(t *testing.T) {
	stack := NewStack[string]()

	// Push items when there are no waiters
	stack.Push("first")
	stack.Push("second")
	stack.Push("third")

	// Pop all items and verify they come in LIFO order
	item1 := <-stack.Pop()
	if item1 != "third" {
		t.Errorf("Expected 'third', got '%s'", item1)
	}

	item2 := <-stack.Pop()
	if item2 != "second" {
		t.Errorf("Expected 'second', got '%s'", item2)
	}

	item3 := <-stack.Pop()
	if item3 != "first" {
		t.Errorf("Expected 'first', got '%s'", item3)
	}

	// Verify stack is now empty
	if !stack.IsEmpty() {
		t.Errorf("Expected stack to be empty")
	}
}
