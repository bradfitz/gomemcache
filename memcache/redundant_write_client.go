package memcache

import (
	"bufio"
	"log"
)

type RedundantWriteClient struct {
	Client
}

// New returns a memcache client using the provided server(s)
// if there are multiple servers, each write operation occurs
// on each one for redundancy
func NewWithRedundancy(server ...string) *RedundantWriteClient {
	ss := new(ServerList)
	ss.SetServers(server...)
	return NewRedundantClientFromSelector(ss)
}

func NewRedundantClientFromSelector(ss ServerSelector) *RedundantWriteClient {
	return &RedundantWriteClient{Client{selector: ss}}
}

func (c *RedundantWriteClient) Set(item *Item) error {
	return c.onItem(item, (*RedundantWriteClient).set)
}

func (c *RedundantWriteClient) Add(item *Item) error {
	return c.onItem(item, (*RedundantWriteClient).add)
}

func (c *RedundantWriteClient) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*RedundantWriteClient).cas)
}

func (c *RedundantWriteClient) onItem(item *Item, fn func(*RedundantWriteClient, *bufio.ReadWriter, *Item) error) error {
	ss := c.selector.(*ServerList)
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	if len(ss.addrs) == 0 {
		return ErrNoServers
	}
	var failCount = 0
	var err error
	var cn *conn
	for _, addr := range ss.addrs {
		cn, err = c.getConn(addr)
		if err != nil {
			log.Printf("[memcache] Operation failed on key = %s, err = %v", item.Key, err)
			failCount += 1
			continue
		}
		defer cn.condRelease(&err)
		if err = fn(c, cn.rw, item); err != nil {
			log.Printf("[memcache] Operation failed on key = %s, err = %v", item.Key, err)
			failCount += 1
		}
	}
	if failCount >= len(ss.addrs) {
		return err
	}
	return nil
}

func (c *RedundantWriteClient) Delete(key string) error {
	ss := c.selector.(*ServerList)
	var failCount = 0
	var err error
	for _, addr := range ss.addrs {
		err = c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
			return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
		})
		if err != nil {
			log.Printf("[memcache] Delete operation failed on key = %s, err = %v", key, err)
			failCount += 1
		}
	}
	if failCount == len(ss.addrs) {
		return err
	}
	return nil
}

func (c *RedundantWriteClient) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	var err error
	ss := c.selector.(*ServerList)
	for _, addr := range ss.addrs {
		err = c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
			var err error
			val, err = c._incrDecr(rw, verb, key, delta)
			return err
		})
	}
	return val, err
}
