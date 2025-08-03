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

package concurrency

// Pool manages the available goroutine slots for concurrent execution
type Pool struct {
	semaphore chan struct{}
}

// NewPool creates a new concurrency pool
// If maxConcurrency <= 1, returns a pool that doesn't allow concurrent
// execution
// Otherwise returns a pool with maxConcurrency-1 available slots (since
// the main goroutine counts as 1)
func NewPool(maxConcurrency int) *Pool {
	if maxConcurrency <= 1 {
		return &Pool{}
	}
	return &Pool{
		semaphore: make(chan struct{}, maxConcurrency-1),
	}
}

// Release releases a concurrency slot back to the pool
// This method is safe to call even if no slot was acquired
func (c *Pool) Release() {
	if c.semaphore != nil {
		select {
		case <-c.semaphore:
			// Successfully released a slot
		default:
			// No slot to release (semaphore is full), which is fine
		}
	}
}

// TryAcquire attempts to acquire a concurrency slot
// Returns true if a slot was acquired, false if no slots are available
func (c *Pool) TryAcquire() bool {
	if c.semaphore == nil {
		return false
	}

	select {
	case c.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}
