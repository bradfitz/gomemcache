/*
Copyright 2011 The gomemcache AUTHORS

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
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var debug = flag.Bool("debug", false, "be more verbose")

const localhostTCPAddr = "localhost:11211"

func TestLocalhost(t *testing.T) {
	t.Parallel()
	c, err := net.Dial("tcp", localhostTCPAddr)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddr)
	}
	io.WriteString(c, "flush_all\r\n")
	c.Close()

	testWithClient(t, New(localhostTCPAddr))
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	t.Parallel()
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d.sock", os.Getpid())
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		t.Skipf("skipping test; couldn't find memcached")
		return
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	testWithClient(t, New(sock))
}

func TestFakeServer(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Logf("running test server on %s", ln.Addr())
	defer ln.Close()
	srv := &testServer{}
	go srv.Serve(ln)

	testWithClient(t, New(ln.Addr().String()))
}

func TestTLS(t *testing.T) {
	t.Parallel()
	td := t.TempDir()

	// Test whether our memcached binary has TLS support. We --enable-ssl first,
	// before --version, as memcached evaluates the flags in the order provided
	// and we want it to fail if it's built without TLS support (as it is in
	// Debian, but not Ubuntu or Homebrew).
	out, err := exec.Command("memcached", "--enable-ssl", "--version").CombinedOutput()
	if err != nil {
		t.Skipf("skipping test; couldn't find memcached or no TLS support in binary: %v, %s", err, out)
	}
	t.Logf("version: %s", bytes.TrimSpace(out))

	if err := os.WriteFile(filepath.Join(td, "/cert.pem"), LocalhostCert, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(td, "/key.pem"), LocalhostKey, 0644); err != nil {
		t.Fatal(err)
	}

	// Find some unused port. This is racy but we hope for the best and hope the kernel
	// doesn't reassign our ephemeral port to somebody in the tiny race window.
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.Command("memcached",
		"--port="+strconv.Itoa(port),
		"--listen=127.0.0.1",
		"--enable-ssl",
		"-o", "ssl_chain_cert=cert.pem",
		"-o", "ssl_key=key.pem")
	cmd.Dir = td
	if *debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start memcached: %v", err)
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Wait a bit for the server to be running.
	for i := 0; i < 10; i++ {
		nc, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
		if err == nil {
			t.Logf("localhost:%d is up.", port)
			nc.Close()
			break
		}
		t.Logf("waiting for localhost:%d to be up...", port)
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	c := New(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	c.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var td tls.Dialer
		td.Config = &tls.Config{
			InsecureSkipVerify: true,
		}
		return td.DialContext(ctx, network, addr)

	}
	testWithClient(t, c)
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
		t.Helper()
		if err != nil {
			t.Fatalf(format, args...)
		}
	}
	mustSet := mustSetF(t, c)

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval-fromset"), Flags: 123}
	err := c.Set(foo)
	checkErr(err, "first set(foo): %v", err)
	err = c.Set(foo)
	checkErr(err, "second set(foo): %v", err)

	// CompareAndSwap
	it, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if string(it.Value) != "fooval-fromset" {
		t.Errorf("get(foo) Value = %q, want fooval-romset", it.Value)
	}
	it0, err := c.Get("foo") // another get, to fail our CAS later
	checkErr(err, "get(foo): %v", err)
	it.Value = []byte("fooval")
	err = c.CompareAndSwap(it)
	checkErr(err, "cas(foo): %v", err)
	it0.Value = []byte("should-fail")
	if err := c.CompareAndSwap(it0); err != ErrCASConflict {
		t.Fatalf("cas(foo) error = %v; want ErrCASConflict", err)
	}

	// Get
	it, err = c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", it.Value)
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
	malFormed = &Item{Key: "foo" + string(rune(0x7f)), Value: []byte("foobarval")}
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

	// Append
	append := &Item{Key: "append", Value: []byte("appendval")}
	if err := c.Append(append); err != ErrNotStored {
		t.Fatalf("first append(append) want ErrNotStored, got %v", err)
	}
	c.Set(append)
	err = c.Append(&Item{Key: "append", Value: []byte("1")})
	checkErr(err, "second append(append): %v", err)
	appended, err := c.Get("append")
	checkErr(err, "third append(append): %v", err)
	if string(appended.Value) != string(append.Value)+"1" {
		t.Fatalf("Append: want=append1, got=%s", string(appended.Value))
	}

	// Prepend
	prepend := &Item{Key: "prepend", Value: []byte("prependval")}
	if err := c.Prepend(prepend); err != ErrNotStored {
		t.Fatalf("first prepend(prepend) want ErrNotStored, got %v", err)
	}
	c.Set(prepend)
	err = c.Prepend(&Item{Key: "prepend", Value: []byte("1")})
	checkErr(err, "second prepend(prepend): %v", err)
	prepended, err := c.Get("prepend")
	checkErr(err, "third prepend(prepend): %v", err)
	if string(prepended.Value) != "1"+string(prepend.Value) {
		t.Fatalf("Prepend: want=1prepend, got=%s", string(prepended.Value))
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

	// Test Ping
	err = c.Ping()
	checkErr(err, "error ping: %s", err)
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
	if err == nil {
		t.Fatalf("item bar did not expire within %v seconds", time.Now().Sub(setTime).Seconds())
	} else {
		if err != ErrCacheMiss {
			t.Fatalf("unexpected error retrieving bar: %v", err.Error())
		}
	}
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
