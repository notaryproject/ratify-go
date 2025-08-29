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
)

func TestSlice_Add(t *testing.T) {
	slice := NewSlice[int]()

	slice.Add(1)
	slice.Add(2)
	slice.Add(3)

	items := slice.Get()
	if len(items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(items))
	}

	expectedItems := []int{1, 2, 3}
	for i, item := range items {
		if item != expectedItems[i] {
			t.Errorf("Expected item %d to be %d, got %d", i, expectedItems[i], item)
		}
	}
}

func TestSlice_Get(t *testing.T) {
	slice := NewSlice[string]()

	// Test empty slice
	items := slice.Get()
	if len(items) != 0 {
		t.Errorf("Expected empty slice, got %d items", len(items))
	}

	slice.Add("hello")
	slice.Add("world")

	items = slice.Get()
	if len(items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(items))
	}

	// Verify the returned slice is a copy
	items[0] = "modified"
	originalItems := slice.Get()
	if originalItems[0] == "modified" {
		t.Error("Get() should return a copy, not the original slice")
	}
}

func TestSlice_Concurrent(t *testing.T) {
	slice := NewSlice[int]()
	var wg sync.WaitGroup

	// Start multiple goroutines to add items concurrently
	numGoroutines := 10
	itemsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				slice.Add(start*itemsPerGoroutine + j)
			}
		}(i)
	}

	wg.Wait()

	items := slice.Get()
	expectedLength := numGoroutines * itemsPerGoroutine
	if len(items) != expectedLength {
		t.Errorf("Expected %d items, got %d", expectedLength, len(items))
	}

	// Verify all items are present (order may vary due to concurrency)
	itemMap := make(map[int]bool)
	for _, item := range items {
		itemMap[item] = true
	}

	for i := 0; i < expectedLength; i++ {
		if !itemMap[i] {
			t.Errorf("Item %d is missing", i)
		}
	}
}
