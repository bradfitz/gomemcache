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
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
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

	// ErrServerError means the server replied "SERVER_ERROR <message>" to a
	// request. Errors returned by this package match it with errors.Is; use
	// errors.As with *ServerError to recover the server's message.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 500 * time.Millisecond

	// DefaultDialTimeout is the default per-attempt connect timeout.
	DefaultDialTimeout = 5 * time.Second

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

// ServerError is the error returned when the server replies
// "SERVER_ERROR <message>" to a request. Per the memcached protocol, such
// an error applies only to the request that provoked it, so the connection
// remains open and any requests pipelined behind it are unaffected.
// It matches ErrServerError under errors.Is.
type ServerError struct {
	// Message is the text the server sent after the SERVER_ERROR prefix.
	Message string
}

func (e *ServerError) Error() string { return "memcache: server error: " + e.Message }

// Is reports whether e matches target. It exists so that
// errors.Is(err, ErrServerError) is true for any *ServerError.
func (e *ServerError) Is(target error) bool { return target == ErrServerError }

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	if _, ok := err.(*ServerError); ok {
		return true
	}
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
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultErrorLine         = []byte("ERROR\r\n")
	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
	versionPrefix           = []byte("VERSION")
)

// protocolError maps one of the protocol's generic error response lines
// (ERROR, CLIENT_ERROR, SERVER_ERROR) to a Go error. It returns nil if
// line is not one of them.
//
// SERVER_ERROR maps to a *ServerError, which is resumable (see
// resumableError): the protocol defines it as failing only the request
// that provoked it, so the connection stays open. ERROR and CLIENT_ERROR
// mean the server rejected the request itself and the connection's
// request/response framing can no longer be trusted, so they map to
// non-resumable errors and the connection is closed, failing over any
// requests pipelined behind the bad one.
func protocolError(line []byte) error {
	msg := func(prefix []byte) string {
		return string(bytes.TrimRight(line[len(prefix):], "\r\n"))
	}
	switch {
	case bytes.HasPrefix(line, resultServerErrorPrefix):
		return &ServerError{Message: msg(resultServerErrorPrefix)}
	case bytes.HasPrefix(line, resultClientErrorPrefix):
		return errors.New("memcache: client error: " + msg(resultClientErrorPrefix))
	case bytes.Equal(line, resultErrorLine):
		return errors.New("memcache: ERROR reply from server (bad command)")
	}
	return nil
}

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
	// DialContext connects to the address on the named network using the
	// provided context.
	//
	// To connect to servers using TLS (memcached running with "--enable-ssl"),
	// use a DialContext func that uses tls.Dialer.DialContext. See this
	// package's tests as an example.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)

	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// DialTimeout is the per-attempt timeout for establishing a new
	// connection to a server. It is intentionally separate from Timeout
	// because the connection that a dial produces may be reused by many
	// later operations whose individual contexts have nothing to do with
	// how long this dial is allowed to take.
	//
	// If zero, DefaultDialTimeout is used.
	DialTimeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections kept
	// warm per address after a period of inactivity. A background reaper
	// wakes after ~5s of quiescence on a backend and closes idle conns
	// beyond this cap. If less than one, DefaultMaxIdleConns is used.
	MaxIdleConns int

	// MaxPipelineDepth is the maximum number of requests allowed in flight
	// on a single connection at once. Once every connection to a backend
	// is at this cap and no more can be dialed (see MaxConns, MaxDials),
	// subsequent operations block in their submit call until a slot frees.
	//
	// A value of 1 disables pipelining (each connection serves one request
	// at a time, matching the pre-pipelining behavior).
	//
	// A value of 0 selects an implementation-chosen default (currently 8;
	// subject to change).
	MaxPipelineDepth int

	// MaxConns is the maximum number of open connections per server address
	// (in-use + idle). If zero, DefaultMaxConns is used. A negative value
	// means unlimited.
	MaxConns int

	// MaxDials is the maximum number of concurrent dial attempts per server
	// address. If zero, DefaultMaxDials is used. A negative value means
	// unlimited.
	MaxDials int

	selector ServerSelector

	mu       sync.Mutex
	backends map[backendKey]*backend
}

// backendKey identifies a backend without allocating. For TCP addresses
// (the common case) it's a pure netip.AddrPort value; for unix sockets
// or unknown networks, path holds the address's String() value.
type backendKey struct {
	ap   netip.AddrPort
	path string
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

	// CasID is the compare and swap ID.
	//
	// It's populated by get requests and then the same value is
	// required for a CompareAndSwap request to succeed.
	CasID uint64
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) dialTimeout() time.Duration {
	if c.DialTimeout != 0 {
		return c.DialTimeout
	}
	return DefaultDialTimeout
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
	ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout())
	defer cancel()

	dialerContext := c.DialContext
	if dialerContext == nil {
		dialer := net.Dialer{
			Timeout: c.dialTimeout(),
		}
		dialerContext = dialer.DialContext
	}

	nc, err := dialerContext(ctx, addr.Network(), addr.String())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

// getBackend returns (creating if needed) the per-address backend.
func (c *Client) getBackend(addr net.Addr) *backend {
	key := addrBackendKey(addr)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.backends == nil {
		c.backends = make(map[backendKey]*backend)
	}
	if b, ok := c.backends[key]; ok {
		return b
	}
	b := newBackend(c, addr)
	c.backends[key] = b
	return b
}

// addrBackendKey derives a comparable, alloc-free key from a net.Addr.
// TCP-shaped addresses (the common case produced by ServerList) resolve
// to a pure netip.AddrPort with no string conversion. Unix sockets and
// unknown shapes fall back to the address's String() representation.
func addrBackendKey(a net.Addr) backendKey {
	switch v := a.(type) {
	case *net.TCPAddr:
		return backendKey{ap: v.AddrPort()}
	case *staticAddr:
		if v.ap.IsValid() {
			return backendKey{ap: v.ap}
		}
		return backendKey{path: v.str}
	case *net.UnixAddr:
		return backendKey{path: v.Name}
	}
	if a.Network() != "unix" {
		if ap, err := netip.ParseAddrPort(a.String()); err == nil {
			return backendKey{ap: ap}
		}
	}
	return backendKey{path: a.String()}
}

// ctxBG is a package-wide context.Background() to avoid the small per-call
// allocation cost of context.Background() in the non-context command paths.
var ctxBG = context.Background()

// runCmd submits one operation to addr and waits for its completion.
// verb is used to classify idempotency for retry on conn failure.
// Callers without a context should pass ctxBG.
func (c *Client) runCmd(ctx context.Context, addr net.Addr, verb string,
	write func(*bufio.Writer) error,
	read func(*bufio.Reader) error) error {
	req := newPipeReq(verb, write, read)
	return c.getBackend(addr).submit(ctx, req)
}

// runCmdKey picks the server for key (after a legality check) and runs the op.
func (c *Client) runCmdKey(ctx context.Context, key, verb string,
	write func(*bufio.Writer) error,
	read func(*bufio.Reader) error) error {
	if !legalKey(key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return err
	}
	return c.runCmd(ctx, addr, verb, write, read)
}

func (c *Client) FlushAll() error {
	return c.selector.Each(c.flushAllFromAddr)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, []string{key}, func(it *Item) { item = it })
	})
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
		return c.touchFromAddr(addr, key, seconds)
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

func (c *Client) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) error {
	return c.runCmd(ctxBG, addr, "gets",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "gets %s\r\n", strings.Join(keys, " "))
			return err
		},
		func(r *bufio.Reader) error {
			return parseGetResponse(r, cb)
		})
}

// flushAllFromAddr sends the flush_all command to the given addr.
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.runCmd(ctxBG, addr, "flush_all",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "flush_all\r\n")
			return err
		},
		func(r *bufio.Reader) error {
			line, err := r.ReadSlice('\n')
			if err != nil {
				return err
			}
			if !bytes.Equal(line, resultOk) {
				if err := protocolError(line); err != nil {
					return err
				}
				return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
			}
			return nil
		})
}

// ping sends the version command to the given addr.
func (c *Client) ping(addr net.Addr) error {
	return c.runCmd(ctxBG, addr, "version",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "version\r\n")
			return err
		},
		func(r *bufio.Reader) error {
			line, err := r.ReadSlice('\n')
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(line, versionPrefix) {
				if err := protocolError(line); err != nil {
					return err
				}
				return fmt.Errorf("memcache: unexpected response line from ping: %q", string(line))
			}
			return nil
		})
}

// touchFromAddr sends a single touch for key to addr.
func (c *Client) touchFromAddr(addr net.Addr, key string, expiration int32) error {
	return c.runCmd(ctxBG, addr, "touch",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "touch %s %d\r\n", key, expiration)
			return err
		},
		func(r *bufio.Reader) error {
			line, err := r.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				return nil
			case bytes.Equal(line, resultNotFound):
				return ErrCacheMiss
			default:
				if err := protocolError(line); err != nil {
					return err
				}
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	var mu sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		mu.Lock()
		defer mu.Unlock()
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

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item.
//
// The conn-level deadline is set once by the pipeline reader when the
// request is dispatched; a huge response may need Client.Timeout to be
// raised accordingly.
func parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		// The server terminates a get response with an error line in place
		// of END when the request fails partway.
		if err := protocolError(line); err != nil {
			return err
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
	errf := func(line []byte) (int, error) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	if !bytes.HasPrefix(line, []byte("VALUE ")) || !bytes.HasSuffix(line, []byte("\r\n")) {
		return errf(line)
	}
	s := string(line[6 : len(line)-2])
	var rest string
	var found bool
	it.Key, rest, found = cut(s, ' ')
	if !found {
		return errf(line)
	}
	val, rest, found := cut(rest, ' ')
	if !found {
		return errf(line)
	}
	flags64, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return errf(line)
	}
	it.Flags = uint32(flags64)
	val, rest, found = cut(rest, ' ')
	size64, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return errf(line)
	}
	if size64 > math.MaxInt { // Can happen if int is 32-bit
		return errf(line)
	}
	if !found { // final CAS ID is optional.
		return int(size64), nil
	}
	it.CasID, err = strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return errf(line)
	}
	return int(size64), nil
}

// cut is similar to strings.Cut in Go 1.18, but sep can only be 1 byte.
func cut(s string, sep byte) (before, after string, found bool) {
	if i := strings.IndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error { return c.populateOne("set", item) }

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error { return c.populateOne("add", item) }

// Replace writes the given item, but only if the server *does*
// already hold data for this key.
func (c *Client) Replace(item *Item) error { return c.populateOne("replace", item) }

// Append appends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Append(item *Item) error { return c.populateOne("append", item) }

// Prepend prepends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Prepend(item *Item) error { return c.populateOne("prepend", item) }

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error { return c.populateOne("cas", item) }

func (c *Client) populateOne(verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return err
	}
	return c.runCmd(ctxBG, addr, verb,
		func(w *bufio.Writer) error {
			var hdrErr error
			if verb == "cas" {
				_, hdrErr = fmt.Fprintf(w, "%s %s %d %d %d %d\r\n",
					verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.CasID)
			} else {
				_, hdrErr = fmt.Fprintf(w, "%s %s %d %d %d\r\n",
					verb, item.Key, item.Flags, item.Expiration, len(item.Value))
			}
			if hdrErr != nil {
				return hdrErr
			}
			if _, err := w.Write(item.Value); err != nil {
				return err
			}
			_, err := w.Write(crlf)
			return err
		},
		func(r *bufio.Reader) error {
			line, err := r.ReadSlice('\n')
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
			if err := protocolError(line); err != nil {
				return err
			}
			return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
		})
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
//
// If a retry occurs on a new connection because the first attempt's
// connection died, a successful original delete followed by a retry will
// see ErrCacheMiss on the retry: safe, but worth noting.
func (c *Client) Delete(key string) error {
	return c.runCmdKey(ctxBG, key, "delete",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "delete %s\r\n", key)
			return err
		},
		expectOneOf("delete", resultDeleted))
}

// DeleteAll deletes all items from the server picked for the empty key.
func (c *Client) DeleteAll() error {
	return c.runCmdKey(ctxBG, "", "flush_all",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "flush_all\r\n")
			return err
		},
		expectOneOf("flush_all", resultDeleted))
}

// expectOneOf returns a response parser that treats OK or expect as success
// and maps the common error lines to their error types.
func expectOneOf(verb string, expect []byte) func(*bufio.Reader) error {
	return func(r *bufio.Reader) error {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOK),
			bytes.Equal(line, expect):
			return nil
		case bytes.Equal(line, resultNotStored):
			return ErrNotStored
		case bytes.Equal(line, resultExists):
			return ErrCASConflict
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		}
		if err := protocolError(line); err != nil {
			return err
		}
		return fmt.Errorf("memcache: unexpected response line from %s: %q", verb, string(line))
	}
}

// GetAndTouch gets and updates the expiry of the given key. The error
// ErrCacheMiss is returned if the item didn't already exist in the cache.
func (c *Client) GetAndTouch(key string, expiration int32) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getAndTouchFromAddr(addr, key, expiration, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

func (c *Client) getAndTouchFromAddr(addr net.Addr, key string, expiration int32, cb func(*Item)) error {
	return c.runCmd(ctxBG, addr, "gat",
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "gat %d %s\r\n", expiration, key)
			return err
		},
		func(r *bufio.Reader) error {
			return parseGetResponse(r, cb)
		})
}

// Ping checks all instances if they are alive. It returns an error if any
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
	err := c.runCmdKey(ctxBG, key, verb,
		func(w *bufio.Writer) error {
			_, err := fmt.Fprintf(w, "%s %s %d\r\n", verb, key, delta)
			return err
		},
		func(r *bufio.Reader) error {
			line, err := r.ReadSlice('\n')
			if err != nil {
				return err
			}
			if bytes.Equal(line, resultNotFound) {
				return ErrCacheMiss
			}
			if err := protocolError(line); err != nil {
				return err
			}
			val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
			return err
		})
	return val, err
}

// Close closes any open connections.
//
// It returns nil. After Close, the Client may still be used.
func (c *Client) Close() error {
	c.mu.Lock()
	backends := c.backends
	c.backends = nil
	c.mu.Unlock()
	for _, b := range backends {
		b.close()
	}
	return nil
}
