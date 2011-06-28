/*
Copyright 2011 Google Inc.

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

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"net"
	"testing"
)

const testServer = "localhost:11211"

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		t.Logf("skipping test; no server running at %s", testServer)
		return false
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestMemcache(t *testing.T) {
	if !setup(t) {
		return
	}
	c := New(testServer)

	foo := &Item{Key: "foo", Value: []byte("bar")}
	if err := c.Set(foo); err != nil {
		t.Fatalf("first set(foo): %v", err)
	}
	if err := c.Set(foo); err != nil {
		t.Fatalf("second set(foo): %v", err)
	}

	bar := &Item{Key: "bar", Value: []byte("bar2")}
	if err := c.Add(bar); err != nil {
		t.Fatalf("first add(foo): %v", err)
	}
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
	}

}
