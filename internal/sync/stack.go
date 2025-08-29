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

import "sync"

// Stack is a thread-safe stack data structure that supports concurrent
// operations.
// It uses a mutex for synchronization and a condition variable for coordinating
// blocking Pop operations. The stack tracks active workers to enable graceful
// shutdown when all work is complete.
type Stack[T any] struct {
	mu            sync.Mutex
	cond          *sync.Cond
	activeWorkers int
	items         []T
	closed        bool
}

// NewStack creates a new concurrency-safe stack.
func NewStack[T any]() *Stack[T] {
	s := &Stack[T]{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Push adds an item to the stack. This operation is non-blocking.
func (s *Stack[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.items = append(s.items, item)
	s.cond.Signal()
}

// Pop removes and returns an item from the stack. This operation is blocking.
// It returns (item, true) if an item was successfully popped, or (zero, false)
// if the stack is closed.
func (s *Stack[T]) Pop() (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.items) == 0 && !s.closed {
		s.cond.Wait()
	}

	if s.closed && len(s.items) == 0 {
		var zero T
		return zero, false
	}
	s.activeWorkers++
	item := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return item, true
}

// Done signals that an item has been processed and a worker is released.
// If the stack is empty and no workers are active, the stack will be closed.
func (s *Stack[T]) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeWorkers--
	if s.activeWorkers == 0 && !s.closed && len(s.items) == 0 {
		s.closed = true
		s.cond.Broadcast()
	}
}
