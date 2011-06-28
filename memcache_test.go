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

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	if err := c.Set(foo); err != nil {
		t.Fatalf("first set(foo): %v", err)
	}
	if err := c.Set(foo); err != nil {
		t.Fatalf("second set(foo): %v", err)
	}

	// Get
	it, err := c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
	}
	if it.Flags != 123 {
		t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	if err := c.Add(bar); err != nil {
		t.Fatalf("first add(foo): %v", err)
	}
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
	}

	// GetMulti
	m, err := c.GetMulti([]string{"foo", "bar"})
	if err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	if g, e := len(m), 2; g != e {
		t.Errorf("GetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m["foo"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'foo'")
	}
	if _, ok := m["bar"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'bar'")
	}
	if g, e := string(m["foo"].Value), "fooval"; g != e {
		t.Errorf("GetMulti: foo: got %q, want %q", g, e)
	}
	if g, e := string(m["bar"].Value), "barval"; g != e {
		t.Errorf("GetMulti: bar: got %q, want %q", g, e)
	}

	// Delete
	err = c.Delete("foo")
	if err != nil {
		t.Errorf("Delete: %v", err)
	}
	it, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
	}

}
