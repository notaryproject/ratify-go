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

package stack

import "testing"

func TestStack(t *testing.T) {
	s := Stack[int]{}
	if s.Len() != 0 {
		t.Errorf("Expected stack to be empty")
	}

	s.Push(1, 2)
	x := s.Pop()
	if x != 2 {
		t.Errorf("Expected key to be 2")
	}

	x = s.Pop()
	if x != 1 {
		t.Errorf("Expected key to be 1")
	}

	if s.Len() != 0 {
		t.Errorf("Expected stack to be empty")
	}
}

func TestStack_PopEmpty(t *testing.T) {
	s := Stack[int]{}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic when popping from empty stack")
		}
	}()
	s.Pop() // This should panic
}
