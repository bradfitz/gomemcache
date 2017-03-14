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
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const testServer = "localhost:11211"

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", testServer)
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}
	testWithClient(t, New(testServer))
}

func MakeUnixSocketMemcached(t *testing.T, tag string) (string, *exec.Cmd, error) {
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d%s.sock", tag, os.Getpid())
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		t.Skipf("skipping test; couldn't find memcached")
		return "", nil, err
	}
	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}
	return sock, cmd, nil
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	sock, cmd, err := MakeUnixSocketMemcached(t, "")
	if err != nil {
		return
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()
	testWithClient(t, New(sock))
}

func mustSetF(t *testing.T, c *Client) func(*Item) {
	return func(it *Item) {
		if err := c.Set(it); err != nil {
			t.Fatalf("failed to Set %#v: %v", *it, err)
		}
	}
}

func testWithClient(t *testing.T, c *Client) {
	checkErr := func(err error, format string, args ...interface{}) {
		if err != nil {
			t.Fatalf(format, args...)
		}
	}
	mustSet := mustSetF(t, c)

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	err := c.Set(foo)
	checkErr(err, "first set(foo): %v", err)
	err = c.Set(foo)
	checkErr(err, "second set(foo): %v", err)

	// Get
	it, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
	}
	if it.Flags != 123 {
		t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
	}

	// Get and set a unicode key
	quxKey := "Hello_世界"
	qux := &Item{Key: quxKey, Value: []byte("hello world")}
	err = c.Set(qux)
	checkErr(err, "first set(Hello_世界): %v", err)
	it, err = c.Get(quxKey)
	checkErr(err, "get(Hello_世界): %v", err)
	if it.Key != quxKey {
		t.Errorf("get(Hello_世界) Key = %q, want Hello_世界", it.Key)
	}
	if string(it.Value) != "hello world" {
		t.Errorf("get(Hello_世界) Value = %q, want hello world", string(it.Value))
	}

	// Set malformed keys
	malFormed := &Item{Key: "foo bar", Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
	}
	malFormed = &Item{Key: "foo" + string(0x7f), Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	checkErr(err, "first add(foo): %v", err)
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
	}

	// Replace
	baz := &Item{Key: "baz", Value: []byte("bazvalue")}
	if err := c.Replace(baz); err != ErrNotStored {
		t.Fatalf("expected replace(baz) to return ErrNotStored, got %v", err)
	}
	err = c.Replace(bar)
	checkErr(err, "replaced(foo): %v", err)

	// GetMulti
	m, err := c.GetMulti([]string{"foo", "bar"})
	checkErr(err, "GetMulti: %v", err)
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
	checkErr(err, "Delete: %v", err)
	it, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
	}

	// Incr/Decr
	mustSet(&Item{Key: "num", Value: []byte("42")})
	n, err := c.Increment("num", 8)
	checkErr(err, "Increment num + 8: %v", err)
	if n != 50 {
		t.Fatalf("Increment num + 8: want=50, got=%d", n)
	}
	n, err = c.Decrement("num", 49)
	checkErr(err, "Decrement: %v", err)
	if n != 1 {
		t.Fatalf("Decrement 49: want=1, got=%d", n)
	}
	err = c.Delete("num")
	checkErr(err, "delete num: %v", err)
	n, err = c.Increment("num", 1)
	if err != ErrCacheMiss {
		t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
	}
	mustSet(&Item{Key: "num", Value: []byte("not-numeric")})
	n, err = c.Increment("num", 1)
	if err == nil || !strings.Contains(err.Error(), "client error") {
		t.Fatalf("increment non-number: want client error, got %v", err)
	}
	testTouchWithClient(t, c)

	// Test Delete All
	err = c.DeleteAll()
	checkErr(err, "DeleteAll: %v", err)
	it, err = c.Get("bar")
	if err != ErrCacheMiss {
		t.Errorf("post-DeleteAll want ErrCacheMiss, got %v", err)
	}

}

func testTouchWithClient(t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcache Touch with testing in Short mode")
		return
	}

	mustSet := mustSetF(t, c)

	const secondsToExpiry = int32(2)

	// We will set foo and bar to expire in 2 seconds, then we'll keep touching
	// foo every second
	// After 3 seconds, we expect foo to be available, and bar to be expired
	foo := &Item{Key: "foo", Value: []byte("fooval"), Expiration: secondsToExpiry}
	bar := &Item{Key: "bar", Value: []byte("barval"), Expiration: secondsToExpiry}

	setTime := time.Now()
	mustSet(foo)
	mustSet(bar)

	for s := 0; s < 3; s++ {
		time.Sleep(time.Duration(1 * time.Second))
		err := c.Touch(foo.Key, secondsToExpiry)
		if nil != err {
			t.Errorf("error touching foo: %v", err.Error())
		}
	}

	_, err := c.Get("foo")
	if err != nil {
		if err == ErrCacheMiss {
			t.Fatalf("touching failed to keep item foo alive")
		} else {
			t.Fatalf("unexpected error retrieving foo after touching: %v", err.Error())
		}
	}

	_, err = c.Get("bar")
	if nil == err {
		t.Fatalf("item bar did not expire within %v seconds", time.Now().Sub(setTime).Seconds())
	} else {
		if err != ErrCacheMiss {
			t.Fatalf("unexpected error retrieving bar: %v", err.Error())
		}
	}
}

// stallProxy blocks all connections until a caller unblocks it.
type stallProxy struct {
	l       *net.UnixListener
	stallCh chan struct{}
	sock    string
	mu      sync.Mutex
	waiting int
}

func newStallProxy(addr string) (*stallProxy, error) {
	sock := fmt.Sprintf("/tmp/test-stall-gomemcache-%d.sock", os.Getpid())
	laddr, err := net.ResolveUnixAddr("unix", sock)
	if err != nil {
		return nil, err
	}
	l, err := net.ListenUnix("unix", laddr)
	if err != nil {
		return nil, err
	}
	s := &stallProxy{
		stallCh: make(chan struct{}, 1000),
		l:       l,
		sock:    sock,
	}
	go s.listenLoop(addr)
	return s, nil
}

func (s *stallProxy) listenLoop(addr string) {
	laddr, err := net.ResolveUnixAddr("unix", addr)
	if err != nil {
		log.Fatalf("ResolveUnixAddr(%s) failed: %v", addr, err)
	}
	for {
		lConn, err := s.l.AcceptUnix()
		if err != nil {
			log.Fatalf("AcceptUnix(%s) failed: %v", addr, err)
		}
		rConn, err := net.DialUnix("unix", nil, laddr)
		if err != nil {
			log.Fatalf("Dial(%s) failed: %v", addr, err)
		}
		go func() { // Send loop
			io.Copy(rConn, lConn)
			rConn.CloseWrite()
			lConn.CloseRead()
		}()
		go func() { // Receive loop
			s.mu.Lock()
			s.waiting++
			s.mu.Unlock()
			<-s.stallCh
			io.Copy(lConn, rConn)
			rConn.CloseRead()
			lConn.CloseWrite()
		}()
	}
}

func (s *stallProxy) Close() {
	s.l.Close()
}

func (s *stallProxy) Unstall() {
	close(s.stallCh)
	return
	s.mu.Lock()
	for s.waiting > 0 {
		s.stallCh <- struct{}{}
		s.waiting--
	}
	s.mu.Unlock()
}

func (s *stallProxy) Waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiting
}

// Run a stallProxy in front of a unix domain socket to memacached.
func TestMaxConn(t *testing.T) {
	sock, cmd, err := MakeUnixSocketMemcached(t, "-max")
	if err != nil {
		return
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	stallp, err := newStallProxy(sock)
	c := New(stallp.sock)
	n := 10
	c.MaxOpenConns = n
	c.Timeout = 1 * time.Second
	mustSet := mustSetF(t, c)
	// Start 2 * MaxOpenConns operations.
	var wg sync.WaitGroup
	for i := 0; i < n*2; i++ {
		wg.Add(1)
		go func(a int) {
			mustSet(&Item{Key: "foo", Value: []byte("42")})
			wg.Done()
		}(i)
	}
	// Wait and verify n are queued.
	time.Sleep(c.Timeout / 2)
	if stallp.Waiting() != n {
		t.Fatalf("Incorrect number of connections waiting: %d\n", stallp.Waiting())
	}
	stallp.Unstall()
	// Ensure that they all complete successfully.
	wg.Wait()
}

func BenchmarkOnItem(b *testing.B) {
	fakeServer, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal("Could not open fake server: ", err)
	}
	defer fakeServer.Close()
	go func() {
		for {
			if c, err := fakeServer.Accept(); err == nil {
				go func() { io.Copy(ioutil.Discard, c) }()
			} else {
				return
			}
		}
	}()

	addr := fakeServer.Addr()
	c := New(addr.String())
	if _, err := c.getConn(addr); err != nil {
		b.Fatal("failed to initialize connection to fake server")
	}

	item := Item{Key: "foo"}
	dummyFn := func(_ *Client, _ *bufio.ReadWriter, _ *Item) error { return nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.onItem(&item, dummyFn)
	}
}
