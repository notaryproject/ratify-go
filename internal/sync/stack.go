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

	"github.com/notaryproject/ratify-go/internal/stack"
)

// Stack represents a concurrency-safe stack implementation.
// It wraps up a standard stack and provides additional synchronization 
// primitives.
// It makes accessing the underlying stack and activeWorkers as atomic.
// Push operations are non-blocking and directly add items to the stack.
// Pop operations are blocking and use sync.Cond to wait for items.
type Stack[T any] struct {
	mu            sync.Mutex
	cond          *sync.Cond
	activeWorkers int
	stack         *stack.Stack[T]
	closed        bool
}

// NewStack creates a new concurrency-safe stack.
func NewStack[T any]() *Stack[T] {
	s := &Stack[T]{
		stack: &stack.Stack[T]{},
	}
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
	s.stack.Push(item)
	s.cond.Signal()
}

// Pop removes and returns an item from the stack. This operation is blocking.
// It returns (item, true) if an item was successfully popped, or (zero, false)
// if the stack is closed.
func (s *Stack[T]) Pop() (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.stack.Len() == 0 && !s.closed {
		s.cond.Wait()
	}

	if s.closed && s.stack.Len() == 0 {
		var zero T
		return zero, false
	}
	s.activeWorkers++
	return s.stack.Pop(), true
}

// Done signals that an item has been processed and a worker is released.
// If the stack is empty and no workers are active, the stack will be closed.
func (s *Stack[T]) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeWorkers--
	if s.activeWorkers == 0 && !s.closed && s.stack.Len() == 0 {
		s.closed = true
		s.cond.Broadcast()
	}
}
