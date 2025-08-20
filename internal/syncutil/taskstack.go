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
	"sync"

	"github.com/notaryproject/ratify-go/internal/stack"
)

// taskStack is a stack-based task queue that allows tasks to be pushed onto a stack
// and then popped through a channel.
type taskStack[Task any] struct {
	mu    sync.Mutex
	ch    chan Task
	stack stack.Stack[Task]

	// drainingOnce is used to ensure that the drainStack goroutine is started only once
	drainingOnce    sync.Once
	waitingDraining sync.WaitGroup

	closed bool
}

// NewTaskStack creates a new TaskStack with the specified channel buffer size.
func NewTaskStack[Task any](bufferSize int) *taskStack[Task] {
	if bufferSize < 1 {
		bufferSize = 1
	}
	return &taskStack[Task]{
		ch: make(chan Task, bufferSize),
	}
}

// Push adds a task to the stack.
func (s *taskStack[Task]) Push(task Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.stack.Push(task)

	s.drainingOnce.Do(func() {
		s.waitingDraining.Add(1)
		go s.drainStack()
	})
}

// drainStack continuously moves tasks from the stack to the channel
func (s *taskStack[Task]) drainStack() {
	defer s.waitingDraining.Done()
	for {
		s.mu.Lock()
		if s.stack.Len() == 0 || s.closed {
			// reset to allow future pushes to restart draining
			s.drainingOnce = sync.Once{}
			s.mu.Unlock()
			return
		}

		task := s.stack.Pop()
		s.mu.Unlock()

		s.ch <- task
	}
}

// Channel returns a channel that can be used to receive tasks from the stack.
func (s *taskStack[Task]) Channel() <-chan Task {
	return s.ch
}

// Close closes the channel and stops the draining goroutine.
func (s *taskStack[Task]) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	if len(s.ch) == cap(s.ch) {
		// unblock the drainStack goroutine to finish processing the last task
		<-s.ch
	}
	s.waitingDraining.Wait()
	close(s.ch)
}
