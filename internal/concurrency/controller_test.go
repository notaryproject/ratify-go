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

import (
	"sync"
	"testing"
	"time"
)

func TestPool_SingleThreadMode(t *testing.T) {
	// Test maxConcurrency <= 1 (single thread mode)
	pool := NewPool(1)

	// Should not be able to acquire any slots in single thread mode
	if pool.TryAcquire() {
		t.Error("Should not be able to acquire slot in single thread mode")
	}

	// Release should not panic
	pool.Release()
}

func TestPool_ZeroConcurrency(t *testing.T) {
	pool := NewPool(0)

	// Should not be able to acquire any slots
	if pool.TryAcquire() {
		t.Error("Should not be able to acquire slot with zero concurrency")
	}

	// Release should not panic
	pool.Release()
}

func TestPool_MultipleConcurrency(t *testing.T) {
	maxConcurrency := 3
	pool := NewPool(maxConcurrency)

	// Should be able to acquire maxConcurrency-1 slots
	expectedSlots := maxConcurrency - 1
	acquiredSlots := 0

	for i := 0; i < expectedSlots; i++ {
		if pool.TryAcquire() {
			acquiredSlots++
		}
	}

	if acquiredSlots != expectedSlots {
		t.Errorf("Expected to acquire %d slots, got %d", expectedSlots, acquiredSlots)
	}

	// Should not be able to acquire more slots
	if pool.TryAcquire() {
		t.Error("Should not be able to acquire more slots than available")
	}

	// Release one slot
	pool.Release()

	// Should be able to acquire one more slot
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire slot after release")
	}
}

func TestPool_ConcurrentAccess(t *testing.T) {
	maxConcurrency := 5
	pool := NewPool(maxConcurrency)
	expectedSlots := maxConcurrency - 1

	var wg sync.WaitGroup
	acquisitions := make(chan bool, 20) // Buffer larger than possible acquisitions

	// Start multiple goroutines trying to acquire slots
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if pool.TryAcquire() {
				acquisitions <- true

				// Hold the slot for a short time
				time.Sleep(10 * time.Millisecond)
				pool.Release()
			}
		}()
	}

	wg.Wait()
	close(acquisitions)

	// Count successful acquisitions
	acquiredCount := 0
	for range acquisitions {
		acquiredCount++
	}

	// The number of successfully acquired slots should not exceed the limit
	if acquiredCount > expectedSlots {
		t.Errorf("Too many slots acquired: %d, expected max: %d", acquiredCount, expectedSlots)
	}
}

func TestPool_ReleaseAcquirePattern(t *testing.T) {
	pool := NewPool(3) // 2 available slots

	// Acquire both slots
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire first slot")
	}
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire second slot")
	}

	// Should not be able to acquire third slot
	if pool.TryAcquire() {
		t.Error("Should not be able to acquire third slot")
	}

	// Release one slot
	pool.Release()

	// Should be able to acquire again
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire slot after release")
	}

	// Release both slots
	pool.Release()
	pool.Release()

	// Should be able to acquire both again
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire first slot after release")
	}
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire second slot after release")
	}
}

func TestPool_ExcessiveRelease(t *testing.T) {
	pool := NewPool(3)

	// Acquire one slot
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire slot")
	}

	// Release multiple times (more than acquired)
	pool.Release()
	pool.Release() // This should not panic or cause issues

	// Should still be able to acquire slots normally
	if !pool.TryAcquire() {
		t.Error("Should be able to acquire slot after excessive release")
	}
}
