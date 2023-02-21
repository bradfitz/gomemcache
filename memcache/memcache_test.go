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
	"net"
	"os"
	"os/exec"
	"path"
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
	_, _ = c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}

	c := New(testServer)
	t.Cleanup(c.Close)

	testWithClient(t, c)
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	sock := path.Join(t.TempDir(), fmt.Sprintf("test-gomemcache-%d.sock", os.Getpid()))
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		t.Skipf("skipping test; couldn't find memcached")
		return
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	c := New(sock)
	t.Cleanup(c.Close)

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
		if err != nil {
			t.Fatalf(format, args...)
		}
	}
	mustSet := mustSetF(t, c)

	t.Run("get and set", func(t *testing.T) {
		foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
		err := c.Set(foo)
		checkErr(err, "first set(foo): %v", err)
		err = c.Set(foo)
		checkErr(err, "second set(foo): %v", err)

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
	})

	t.Run("get and set unicode key", func(t *testing.T) {
		quxKey := "Hello_世界"
		qux := &Item{Key: quxKey, Value: []byte("hello world")}
		err := c.Set(qux)
		checkErr(err, "first set(Hello_世界): %v", err)
		it, err := c.Get(quxKey)
		checkErr(err, "get(Hello_世界): %v", err)
		if it.Key != quxKey {
			t.Errorf("get(Hello_世界) Key = %q, want Hello_世界", it.Key)
		}
		if string(it.Value) != "hello world" {
			t.Errorf("get(Hello_世界) Value = %q, want hello world", string(it.Value))
		}
	})

	t.Run("set malformed keys", func(t *testing.T) {
		malFormed := &Item{Key: "foo bar", Value: []byte("foobarval")}
		err := c.Set(malFormed)
		if err != ErrMalformedKey {
			t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
		}
		malFormed = &Item{Key: "foo" + string(rune(0x7f)), Value: []byte("foobarval")}
		err = c.Set(malFormed)
		if err != ErrMalformedKey {
			t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
		}
	})

	t.Run("add", func(t *testing.T) {
		bar := &Item{Key: "bar", Value: []byte("barval")}
		err := c.Add(bar)
		checkErr(err, "first add(foo): %v", err)
		if err := c.Add(bar); err != ErrNotStored {
			t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
		}
	})

	t.Run("replace", func(t *testing.T) {
		baz := &Item{Key: "baz", Value: []byte("bazvalue")}
		if err := c.Replace(baz); err != ErrNotStored {
			t.Fatalf("expected replace(baz) to return ErrNotStored, got %v", err)
		}

		bar := &Item{Key: "bar", Value: []byte("barval")}
		err := c.Replace(bar)
		checkErr(err, "replaced(foo): %v", err)
	})

	t.Run("getmulti", func(t *testing.T) {
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
	})

	t.Run("delete", func(t *testing.T) {
		err := c.Delete("foo")
		checkErr(err, "Delete: %v", err)
		_, err = c.Get("foo")
		if err != ErrCacheMiss {
			t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
		}
	})

	t.Run("incr decr", func(t *testing.T) {
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
		_, err = c.Increment("num", 1)
		if err != ErrCacheMiss {
			t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
		}
		mustSet(&Item{Key: "num", Value: []byte("not-numeric")})
		_, err = c.Increment("num", 1)
		if err == nil || !strings.Contains(err.Error(), "client error") {
			t.Fatalf("increment non-number: want client error, got %v", err)
		}
	})

	t.Run("delete all", func(t *testing.T) {
		err := c.DeleteAll()
		checkErr(err, "DeleteAll: %v", err)
		_, err = c.Get("bar")
		if err != ErrCacheMiss {
			t.Errorf("post-DeleteAll want ErrCacheMiss, got %v", err)
		}
	})

	t.Run("ping", func(t *testing.T) {
		err := c.Ping()
		checkErr(err, "error ping: %s", err)
	})

	t.Run("touch", func(t *testing.T) {
		testTouchWithClient(t, c)
	})

	t.Run("get with allocator", func(t *testing.T) {
		foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
		err := c.Set(foo)
		checkErr(err, "first set(foo): %v", err)

		alloc := newTestAllocator(len(foo.Value) + 2)
		it, err := c.Get("foo", WithAllocator(alloc))
		checkErr(err, "get(foo): %v", err)
		t.Cleanup(func() {
			alloc.Put(&it.Value)
		})

		if it.Key != "foo" {
			t.Errorf("get(foo) Key = %q, want foo", it.Key)
		}
		if string(it.Value) != "fooval" {
			t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
		}
		if alloc.numGets != 1 {
			t.Errorf("get(foo) num gets from Allocator = %d, want 1", alloc.numGets)
		}
	})
}

func testTouchWithClient(t *testing.T, c *Client) {
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
		time.Sleep(1 * time.Second)
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
		t.Fatalf("item bar did not expire within %v seconds", time.Since(setTime).Seconds())
	} else {
		if err != ErrCacheMiss {
			t.Fatalf("unexpected error retrieving bar: %v", err.Error())
		}
	}
}

func TestClient_releaseIdleConnections(t *testing.T) {
	const recentlyUsedThreshold = 2 * time.Second

	getClientWithMinIdleConnsHeadroomPercentage := func(t *testing.T, headroomPercentage float64) *Client {
		c := New(testServer)
		t.Cleanup(c.Close)
		c.MinIdleConnsHeadroomPercentage = headroomPercentage
		c.MaxIdleConns = 100
		c.recentlyUsedConnsThreshold = recentlyUsedThreshold

		return c
	}

	getConn := func(c *Client) *conn {
		addr, err := c.selector.PickServer("test")
		if err != nil {
			t.Fatalf("unexpected error picking server: %v", err.Error())
		}

		connection, err := c.getConn(addr)
		if err != nil {
			t.Fatalf("unexpected error getting connection: %v", err.Error())
		}

		return connection
	}

	countFreeConns := func(c *Client) (recentlyUsed, idle int) {
		c.lk.Lock()
		defer c.lk.Unlock()

		for _, freeConnections := range c.freeconn {
			for _, freeConn := range freeConnections {
				if time.Since(freeConn.idleSince) >= c.recentlyUsedConnsThreshold {
					idle++
				} else {
					recentlyUsed++
				}
			}
		}

		return
	}

	t.Run("noop if there are no free connections", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 50)
		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 0 {
			t.Fatalf("expected %d recently used connections but got %d", 0, numRecentlyUsed)
		}
		if numIdle != 0 {
			t.Fatalf("expected %d idle connections but got %d", 0, numIdle)
		}
	})

	t.Run("should not release recently used connections", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 50)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn1.release()
		conn2.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 2 {
			t.Fatalf("expected %d recently used connections but got %d", 2, numRecentlyUsed)
		}
		if numIdle != 0 {
			t.Fatalf("expected %d idle connections but got %d", 0, numIdle)
		}
	})

	t.Run("should release idle connections while honoring the configured headroom", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 50)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		time.Sleep(recentlyUsedThreshold)
		conn3.release()
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 2 {
			t.Fatalf("expected %d recently used connections but got %d", 2, numRecentlyUsed)
		}
		if numIdle != 1 {
			t.Fatalf("expected %d idle connections but got %d", 1, numIdle)
		}
	})

	t.Run("should release all idle connections if headroom is zero", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 0)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		time.Sleep(recentlyUsedThreshold)
		conn3.release()
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 2 {
			t.Fatalf("expected %d recently used connections but got %d", 2, numRecentlyUsed)
		}
		if numIdle != 0 {
			t.Fatalf("expected %d idle connections but got %d", 0, numIdle)
		}
	})

	t.Run("should not release idle connections if headroom is disabled (negative value)", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, -1)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		time.Sleep(recentlyUsedThreshold)
		conn3.release()
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 2 {
			t.Fatalf("expected %d recently used connections but got %d", 2, numRecentlyUsed)
		}
		if numIdle != 2 {
			t.Fatalf("expected %d idle connections but got %d", 2, numIdle)
		}
	})

	t.Run("should not release idle connections if headroom is 100%", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 100)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		time.Sleep(recentlyUsedThreshold)
		conn3.release()
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 2 {
			t.Fatalf("expected %d recently used connections but got %d", 2, numRecentlyUsed)
		}
		if numIdle != 2 {
			t.Fatalf("expected %d idle connections but got %d", 2, numIdle)
		}
	})

	t.Run("should allow to set an headroom percentage > 100%", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 200)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		conn3.release()
		time.Sleep(recentlyUsedThreshold)
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 1 {
			t.Fatalf("expected %d recently used connections but got %d", 1, numRecentlyUsed)
		}
		if numIdle != 2 {
			t.Fatalf("expected %d idle connections but got %d", 2, numIdle)
		}
	})

	t.Run("should keep all idle connections open if the computed headroom is greater than the number of available free connections", func(t *testing.T) {
		c := getClientWithMinIdleConnsHeadroomPercentage(t, 1000)

		conn1 := getConn(c)
		conn2 := getConn(c)
		conn3 := getConn(c)
		conn4 := getConn(c)

		conn1.release()
		conn2.release()
		conn3.release()
		time.Sleep(recentlyUsedThreshold)
		conn4.release()

		c.releaseIdleConnections()

		numRecentlyUsed, numIdle := countFreeConns(c)
		if numRecentlyUsed != 1 {
			t.Fatalf("expected %d recently used connections but got %d", 1, numRecentlyUsed)
		}
		if numIdle != 3 {
			t.Fatalf("expected %d idle connections but got %d", 3, numIdle)
		}
	})
}

func TestClient_Close_ShouldBeIdempotent(t *testing.T) {
	c := New(testServer)

	// Call Close twice and make sure it doesn't panic the 2nd time.
	c.Close()
	c.Close()
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
				go func() { _, _ = io.Copy(ioutil.Discard, c) }()
			} else {
				return
			}
		}
	}()

	addr := fakeServer.Addr()
	c := New(addr.String())
	b.Cleanup(c.Close)

	if _, err := c.getConn(addr); err != nil {
		b.Fatal("failed to initialize connection to fake server")
	}

	item := Item{Key: "foo"}
	dummyFn := func(_ *Client, _ *bufio.ReadWriter, _ *Item) error { return nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.onItem(&item, dummyFn)
	}
}

func BenchmarkScanGetResponseLine(b *testing.B) {
	line := []byte("VALUE foobar1234 0 4096 1234\r\n")
	var it Item
	for i := 0; i < b.N; i++ {
		_, err := scanGetResponseLine(line, &it)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseGetResponse(b *testing.B) {
	valueSize := 500
	response := strings.NewReader(fmt.Sprintf("VALUE foobar1234 0 %v 1234\r\n%s\r\nEND\r\n", valueSize, strings.Repeat("a", valueSize)))

	opts := newOptions(WithAllocator(newTestAllocator(valueSize + 2)))
	c := &Client{}
	reader := bufio.NewReader(response)

	for i := 0; i < b.N; i++ {
		err := c.parseGetResponse(reader, opts, func(it *Item) {
			opts.Alloc.Put(&it.Value)
		})
		if err != nil {
			b.Fatal(err)
		}
		_, err = response.Seek(0, 0)
		if err != nil {
			b.Fatal(err)
		}
		reader.Reset(response)

	}
}

type testAllocator struct {
	pool    sync.Pool
	maxSize int
	numGets int
	numPuts int
}

func newTestAllocator(maxSize int) *testAllocator {
	return &testAllocator{
		maxSize: maxSize,
		pool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, maxSize)
				return &b
			},
		},
	}
}

func (p *testAllocator) Get(sz int) *[]byte {
	if sz > p.maxSize {
		panic("unexpected allocation size in test allocator")
	}

	p.numGets += 1
	bufPtr := p.pool.Get().(*[]byte)
	return bufPtr
}

func (p *testAllocator) Put(b *[]byte) {
	p.numPuts += 1
	p.pool.Put(b)
}
