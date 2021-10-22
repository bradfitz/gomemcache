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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	typeErrorInt    = errors.New("unexpected type (expected int)")
	typeErrorUint32 = errors.New("unexpected type (expected uint32)")
	typeErrorUint64 = errors.New("unexpected type (expected uint64)")

	unexpectedSuffixError = errors.New("unexpected suffix")
	expectedSpaceError    = errors.New("expected space")
	keyIsEmptyError       = errors.New("key is empty")

	integerStrLen = len(strconv.Itoa(math.MaxInt64)) + 2
	suffixGetCall = []byte("VALUE ")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	space           = byte(' ')
	mn              = []byte("mn\r\n")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	versionPrefix           = []byte("VERSION")
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) *Client {
	ss := new(ServerList)
	ss.SetServers(server...)
	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	selector ServerSelector

	lk       sync.Mutex
	freeconn map[string][]*conn
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= c.maxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	type connError struct {
		cn  net.Conn
		err error
	}

	nc, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	cn.extendDeadline()
	return cn, nil
}

func (c *Client) onItem(item *Item, fn func(*Client, *bufio.ReadWriter, *Item) error) error {
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	if err = fn(c, cn.rw, item); err != nil {
		return err
	}
	return nil
}

func (c *Client) FlushAll() error {
	return c.selector.Each(c.flushAllFromAddr)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		var err error
		item, err = c.GetByAddr(addr, key)
		return err
	})
	return
}

// Get, caller have to be sure that all keys placed on one server
// In method skipped keys validation
func (c *Client) GetByAddr(addr net.Addr, key string) (item *Item, err error) {
	err = c.getFromAddr(addr, []string{key}, func(it *Item) { item = it })
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key string, seconds int32) (err error) {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, []string{key}, seconds)
	})
}

func (c *Client) withKeyAddr(key string, fn func(net.Addr) error) (err error) {
	if !legalKey(key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) withAddrRw(addr net.Addr, fn func(*bufio.ReadWriter) error) (err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	return fn(cn.rw)
}

func (c *Client) withKeyRw(key string, fn func(*bufio.ReadWriter) error) error {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

func (c *Client) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		if err := parseGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "flush_all\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOk):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

// ping sends the version command to the given addr
func (c *Client) ping(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "version\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}

		switch {
		case bytes.HasPrefix(line, versionPrefix):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from ping: %q", string(line))
		}
		return nil
	})
}

func (c *Client) touchFromAddr(addr net.Addr, keys []string, expiration int32) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
				return err
			}
			if err := rw.Flush(); err != nil {
				return err
			}
			line, err := rw.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				return ErrCacheMiss
			default:
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		return nil
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key] = it
	}

	keyMap := make(map[net.Addr][]string)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for _ = range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

// GetMultiByAddr, caller have to be sure that all keys placed on one server
// In method skipped keys validation
func (c *Client) GetMultiByAddr(addr net.Addr, keys []string) (map[string]*Item, error) {
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		m[it.Key] = it
	}

	err := c.getFromAddr(addr, keys, addItemToMap)

	return m, err
}

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	currPos := 0
	if currPos, err = validateSuffix(line); err != nil {
		return
	}
	if it.Key, currPos, err = scanWord(line, currPos); err != nil {
		return
	}
	if currPos, err = scanSpace(line, currPos); err != nil {
		return
	}
	if it.Flags, currPos, err = scanUint32(line, currPos); err != nil {
		return
	}
	if currPos, err = scanSpace(line, currPos); err != nil {
		return
	}
	if size, currPos, err = scanInt(line, currPos); err != nil {
		return
	}
	if currPos < len(line) {
		if currPos, err = scanSpace(line, currPos); err != nil {
			return
		}
		if it.casid, currPos, err = scanUint64(line, currPos); err != nil {
			return
		}
	}
	return size, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func validateSuffix(line []byte) (lastIndexByte int, err error) {
	if len(line) < len(suffixGetCall) {
		return 0, unexpectedSuffixError
	}
	for i, s := range suffixGetCall {
		if line[i] != s {
			return 0, unexpectedSuffixError
		}
	}
	return len(suffixGetCall), nil
}

func scanWord(data []byte, iStart int) (res string, lastIndexByte int, err error) {
	iCurr := iStart
	for ; iCurr < len(data) && data[iCurr] != space; iCurr++ {
	}
	if iCurr == iStart {
		return "", 0, keyIsEmptyError
	}
	return string(data[iStart:iCurr]), iCurr, nil
}

func scanInt(data []byte, iStart int) (res int, lastIndexByte int, err error) {
	var currDestRes int
	iCurr := iStart
	for ; iCurr < min(len(data), iStart+integerStrLen); iCurr++ {
		currByte := data[iCurr]
		if currByte < '0' || '9' < currByte {
			break
		}
		currDestRes = currDestRes*10 + int(currByte-'0')
	}
	if iCurr == iStart {
		return 0, 0, typeErrorInt
	}
	return currDestRes, iCurr, nil
}

func scanUint32(data []byte, iStart int) (res uint32, lastIndexByte int, err error) {
	var currDestRes uint32
	iCurr := iStart
	for ; iCurr < min(len(data), iStart+integerStrLen); iCurr++ {
		currByte := data[iCurr]
		if currByte < '0' || '9' < currByte {
			break
		}
		currDestRes = currDestRes*10 + uint32(currByte-'0')
	}
	if iCurr == iStart {
		return 0, 0, typeErrorUint32
	}
	return currDestRes, iCurr, nil
}

func scanUint64(data []byte, iStart int) (res uint64, lastIndexByte int, err error) {
	var currDestRes uint64
	iCurr := iStart
	for ; iCurr < min(len(data), iStart+integerStrLen); iCurr++ {
		currByte := data[iCurr]
		if currByte < '0' || '9' < currByte {
			break
		}
		currDestRes = currDestRes*10 + uint64(currByte-'0')
	}
	if iCurr == iStart {
		return 0, 0, typeErrorUint64
	}
	return currDestRes, iCurr, nil
}

func scanSpace(data []byte, iStart int) (lastIndexByte int, err error) {
	if iStart >= len(data) || data[iStart] != space {
		return 0, expectedSpaceError
	}
	return iStart + 1, nil
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.onItem(item, (*Client).set)
}

func (c *Client) set(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "set", item)
}

// SetMulti is a batch version of Set
// (memcached <= 1.6.9, uses experimental meta protocol)
func (c *Client) SetMulti(_ context.Context, items []*Item) error {
	itemsMap, err := c.getItemsByServerMap(items)
	if err != nil {
		return nil
	}

	ch := make(chan error, buffered)
	for addr, items := range itemsMap {
		go func(addr net.Addr, items []*Item) {
			ch <- c.setMany(addr, items)
		}(addr, items)
	}

	var responseErr error
	for _ = range itemsMap {
		if ge := <-ch; ge != nil {
			responseErr = ge
		}
	}

	return responseErr
}

// SetMultiV2 is a batch version of Set
// (memcached >= 1.6.10)
func (c *Client) SetMultiV2(_ context.Context, items []*Item) error {
	itemsMap, err := c.getItemsByServerMap(items)
	if err != nil {
		return nil
	}

	ch := make(chan error, buffered)
	for addr, items := range itemsMap {
		go func(addr net.Addr, items []*Item) {
			ch <- c.setManyV2(addr, items)
		}(addr, items)
	}

	var responseErr error
	for _ = range itemsMap {
		if ge := <-ch; ge != nil {
			responseErr = ge
		}
	}

	return responseErr
}

func (c *Client) getItemsByServerMap(items []*Item) (map[net.Addr][]*Item, error) {
	itemsByServer := make(map[net.Addr][]*Item)

	for _, item := range items {
		if !legalKey(item.Key) {
			return nil, ErrMalformedKey
		}

		addr, err := c.selector.PickServer(item.Key)
		if err != nil {
			return nil, err
		}

		itemsByServer[addr] = append(itemsByServer[addr], item)
	}

	return itemsByServer, nil
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.onItem(item, (*Client).add)
}

func (c *Client) add(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.onItem(item, (*Client).replace)
}

func (c *Client) replace(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*Client).cas)
}

func (c *Client) cas(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "cas", item)
}

func (c *Client) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}
	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err := rw.Write(crlf); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func (c *Client) setMany(addr net.Addr, items []*Item) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, item := range items {
			_, err := fmt.Fprintf(rw, "ms %s T%d S%d F%d q\r\n", item.Key, item.Expiration, len(item.Value), item.Flags)
			if err != nil {
				return err
			}
			_, err = rw.Write(item.Value)
			if err != nil {
				return err
			}
			_, err = rw.Write(crlf)
			if err != nil {
				return err
			}
		}
		_, err := rw.Write(mn)
		if err != nil {
			return err
		}
		err = rw.Flush()
		if err != nil {
			return err
		}

		return parseMetaCommandResponse(rw)
	})
}

func (c *Client) setManyV2(addr net.Addr, items []*Item) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, item := range items {
			_, err := fmt.Fprintf(rw, "ms %s %d T%d F%d q\r\n", item.Key, len(item.Value), item.Expiration, item.Flags)
			if err != nil {
				return err
			}
			_, err = rw.Write(item.Value)
			if err != nil {
				return err
			}
			_, err = rw.Write(crlf)
			if err != nil {
				return err
			}
		}
		_, err := rw.Write(mn)
		if err != nil {
			return err
		}
		err = rw.Flush()
		if err != nil {
			return err
		}

		return parseMetaCommandResponse(rw)
	})
}

func parseMetaCommandResponse(rw *bufio.ReadWriter) error {
	for {
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, []byte("MN\r\n")):
			return err
		case bytes.Equal(line, resultNotStored):
			err = ErrNotStored
		default:
			err = fmt.Errorf("unexpected line %s", line)
		}
	}
}

func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	line, err := rw.ReadSlice('\n')
	return line, err
}

func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key string) error {
	return c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
	})
}

// DeleteMulti is a batch version of Delete. It uses experimental Meta
// commands, which may changes.
func (c *Client) DeleteMulti(keys []string) error {
	keysMap := make(map[net.Addr][]string)

	for _, key := range keys {
		if !legalKey(key) {
			return ErrMalformedKey
		}

		addr, err := c.selector.PickServer(key)
		if err != nil {
			return err
		}

		keysMap[addr] = append(keysMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keysMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.deleteMany(addr, keys)
		}(addr, keys)
	}

	var responseErr error
	for _ = range keysMap {
		if ge := <-ch; ge != nil {
			responseErr = ge
		}
	}

	return responseErr
}

func (c *Client) deleteMany(addr net.Addr, keys []string) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			_, err := fmt.Fprintf(rw, "md %s q\r\n", key) // meta delete with "no reply"
			if err != nil {
				return err
			}
		}
		_, err := rw.Write(mn)
		if err != nil {
			return err
		}
		err = rw.Flush()
		if err != nil {
			return err
		}

		return parseMetaCommandResponse(rw)
	})
}

// DeleteAll deletes all items in the cache.
func (c *Client) DeleteAll() error {
	return c.withKeyRw("", func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "flush_all\r\n")
	})
}

// Ping checks all instances if they are alive. Returns error if any
// of them is down.
func (c *Client) Ping() error {
	return c.selector.Each(c.ping)
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *Client) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}
