package memcache

import (
	"testing"
)

func TestRedundantLocalhost(t *testing.T) {
	var servers = []string{"localhost:11211", "localhost:11212"}
	setup(t, servers)
	testWithRedundantClient(t, NewWithRedundancy(servers...))
}

func TestRedundantLocalhostPartialFailure(t *testing.T) {
	var servers = []string{"localhost:11211", "localhost:11213"}
	setup(t, servers)
	testWithRedundantPartialFailureClient(t, NewWithRedundancy(servers...))
}

func TestRedundantLocalhostFullFailure(t *testing.T) {
	var servers = []string{"localhost:11213", "localhost:11214"}
	setup(t, servers)
	testWithRedundantFullFailureClient(t, NewWithRedundancy(servers...))
}

func testWithRedundantClient(t *testing.T, c *RedundantWriteClient) {

	testSetWithClient(t, c)
	// Ensure redundancy
	ss := c.selector.(*ServerList)
	for _, addr := range ss.addrs {
		c.getFromAddr(addr, []string{"foo"}, func(it *Item) {
			if it.Key != "foo" {
				t.Errorf("get(foo) Addr = %v, Key = %q, want foo", addr, it.Key)
			}
			if string(it.Value) != "fooval" {
				t.Errorf("get(foo) Addr = %v, Value = %q, want fooval", addr, string(it.Value))
			}
		})
	}

	testGetWithClient(t, c)

	testAddWithClient(t, c)
	// Ensure redundancy
	for _, addr := range ss.addrs {
		c.getFromAddr(addr, []string{"bar"}, func(it *Item) {
			if it.Key != "bar" {
				t.Errorf("get(bar) Addr = %v, Key = %q, want bar", addr, it.Key)
			}
			if string(it.Value) != "barval" {
				t.Errorf("get(bar) Addr = %v, Value = %q, want barval", addr, string(it.Value))
			}
		})
	}

	testGetMultiWithClient(t, c)

	testDeleteWithClient(t, c)
	// Ensure redundancy
	for _, addr := range ss.addrs {
		c.getFromAddr(addr, []string{"foo"}, func(it *Item) {
			if it != nil {
				t.Errorf("post-Delete Addr = %v want miss", addr)
			}
		})
	}

	testIncrDecrWithClient(t, c)

}

func testWithRedundantPartialFailureClient(t *testing.T, c *RedundantWriteClient) {

	// Set
	testSetWithClient(t, c)

	ss := c.selector.(*ServerList)
	c.getFromAddr(ss.addrs[0], []string{"foo"}, func(it *Item) {
		if it.Key != "foo" {
			t.Errorf("get(foo) Addr = %v, Key = %q, want foo", ss.addrs[0], it.Key)
		}
		if string(it.Value) != "fooval" {
			t.Errorf("get(foo) Addr = %v, Value = %q, want fooval", ss.addrs[0], string(it.Value))
		}
	})
	err := c.getFromAddr(ss.addrs[1], []string{"foo"}, func(it *Item) {
		if it != nil {
			t.Errorf("get(foo) Addr = %v, Key = %q, want miss", ss.addrs[1], it.Key)
		}
	})
	if err == nil {
		t.Errorf("get(foo) Addr = %v, want error", ss.addrs[1])
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	checkErr(t, c, err, "first add(foo): %v", err)

	c.getFromAddr(ss.addrs[0], []string{"bar"}, func(it *Item) {
		if it.Key != "bar" {
			t.Errorf("get(bar) Addr = %v, Key = %q, want bar", ss.addrs[0], it.Key)
		}
		if string(it.Value) != "barval" {
			t.Errorf("get(bar) Addr = %v, Value = %q, want barval", ss.addrs[0], string(it.Value))
		}
	})

	// Delete
	err = c.Delete("foo")
	checkErr(t, c, err, "Delete: %v", err)

	c.getFromAddr(ss.addrs[0], []string{"foo"}, func(it *Item) {
		if it != nil {
			t.Errorf("post-Delete Addr = %v want miss", ss.addrs[0])
		}
	})

}

func testWithRedundantFullFailureClient(t *testing.T, c *RedundantWriteClient) {

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	err := c.Set(foo)
	if err == nil {
		t.Errorf("set(foo), want error")
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	if err == nil {
		t.Errorf("add(bar), want error")
	}

	// Delete
	err = c.Delete("foo")
	if err == nil {
		t.Errorf("add(bar), want error")
	}

}
