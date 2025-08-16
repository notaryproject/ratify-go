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
)

// Stack represents a concurrency-safe stack implementation.
// Push operations are non-blocking and directly send to waiting pop operations
// if available.
// Pop operations can be blocking and return a channel.
// This implementation uses waiters for immediate delivery and an items slice 
// for storage.
type Stack[T any] struct {
	mu      sync.Mutex
	items   []T
	waiters []chan T
}

// NewStack creates a new concurrency-safe stack.
func NewStack[T any]() *Stack[T] {
	return &Stack[T]{
		items:   make([]T, 0),
		waiters: make([]chan T, 0),
	}
}

// Push adds an item to the stack. This operation is completely non-blocking.
// If there are waiting pop operations, the item is sent directly to one of them.
// If no waiters are available, the item is stored in the items slice.
func (s *Stack[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If there are waiters, send the item to the most recent waiter (LIFO)
	if len(s.waiters) > 0 {
		// Get the last waiter (most recent)
		waiter := s.waiters[len(s.waiters)-1]
		s.waiters = s.waiters[:len(s.waiters)-1]

		// Send the item to the waiter (non-blocking since channel is buffered)
		select {
		case waiter <- item:
			// Successfully sent
		default:
			// Defensive: This case should not occur because resultCh is a 
			// buffered channel of size 1, and we only send one value before
			// returning. If this ever happens, we store the item to ensure 
			// non-blocking behavior.
			s.items = append(s.items, item)
		}
		return
	}

	// No waiters available, store the item in the slice
	s.items = append(s.items, item)
}

// Pop removes and returns an item from the stack through a channel.
// This operation can be blocking if the stack is empty.
func (s *Stack[T]) Pop() <-chan T {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a buffered channel for the result
	resultCh := make(chan T, 1)

	// If there are stored items, return the most recent one immediately (LIFO)
	if len(s.items) > 0 {
		item := s.items[len(s.items)-1]
		s.items = s.items[:len(s.items)-1]
		resultCh <- item
		return resultCh
	}

	// No stored items, add this pop operation to the waiters
	s.waiters = append(s.waiters, resultCh)

	return resultCh
}

// IsEmpty returns true if the stack is empty.
func (s *Stack[T]) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.items) == 0
}

// Close closes the stack and signals all waiting operations.
func (s *Stack[T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all waiting channels
	for _, waiter := range s.waiters {
		close(waiter)
	}
	s.waiters = nil
	s.items = nil
}
