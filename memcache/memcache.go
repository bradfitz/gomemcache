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
	"errors"
	"fmt"
	"io"
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
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2

	// Meta commands flag constants
	base64MetaFlag                        = "b"
	casTokenResponseMetaFlag              = "c"
	itemValueMetaFlag                     = "v"
	ttlResponseMetaFlag                   = "t"
	updateTTLTokenMetaFlag                = "T"
	returnKeyAsTokenMetaFlag              = "k"
	clientFlagsResponseMetaFlag           = "f"
	itemHitResponseMetaFlag               = "h"
	lastAccessTimeResponseMetaFlag        = "l"
	itemSizeResponseMetaFlag              = "s"
	opaqueTokenMetaFlag                   = "O"
	reCacheWonResponseMetaFlag            = "W"
	itemStaleResponseMetaFlag             = "X"
	reCacheWonAlreadySentResponseMetaFlag = "Z"
	vivifyOnMissTokenMetaFlag             = "N"
	reCacheTTLTokenMetaFlag               = "R"
	dontBumpItemInLruMetaFlag             = "u"
	setClientFlagsToTokenMetaFlag         = "F"
	invalidateMetaFlag                    = "I"
	noReplySemanticsMetaFlag              = "q"
	modeTokenMetaFlag                     = "M"
	compareCasValueTokenMetaFlag          = "C"
	autoCreateItemOnMissTokenMetaFlag     = "N"
	initialValueTokenMetaFlag             = "J"
	deltaTokenMetaFlag                    = "D"
	expirationTimeKey                     = "exp"
	lastAccessTimeKey                     = "la"
	casIdKey                              = "cas"
	fetchKey                              = "fetch"
	slabClassIdKey                        = "cls"
	sizeKey                               = "size"
)

const buffered = 8 // arbitrary buffered channel size, for readability

/*
Enum meant to be used with the mode flag in the meta set command- these are
currently the only supported modes by memcached and it forces the user to adhere
to these modes specifically when using the mode flag.

Description of what the different modes do can be found in the SetModeToken attribute
of the MetaSetFlag struct.
*/
type MetaSetMode string

const (
	Add     MetaSetMode = "E"
	Append  MetaSetMode = "A"
	Prepend MetaSetMode = "P"
	Replace MetaSetMode = "R"
	Set     MetaSetMode = "S"
)

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

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
	resultErrorPrefix       = []byte("ERROR")
	versionPrefix           = []byte("VERSION")

	metaValue           = []byte("VA")
	metaCacheMiss       = []byte("EN")
	metaResultStored    = []byte("HD")
	metaResultNotStored = []byte("NS")
	metaResultExists    = []byte("EX")
	metaResultNotFound  = []byte("NF")
	metaResultDeleted   = []byte("HD")
	metaDebugSuccess    = []byte("ME")
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

//MetaArithmeticMode flags mention the mode of meta arithmetic operation
//see ArithmeticModeToken in MetaArithmeticFlags for usage
type MetaArithmeticMode string

const (
	MetaArithmeticIncrement MetaArithmeticMode = "I"
	MetaArithmeticDecrement MetaArithmeticMode = "D"
)

//Flags for MetaArithmetic command
type MetaArithmeticFlags struct {

	//Should be true if the key passed to MetaArithmetic is in base64Format.
	//equivalent to the b flag
	IsKeyBase64 bool

	// use if only want to store a value if the supplied token matches the current CAS token of the item
	// equivalent to the C<token> flag
	CompareCasTokenToUpdateValue *uint64

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	//equivalent to the N<token> flag
	AutoCreateItemOnMissTTLToken *int32

	//An unsigned 64-bit integer which will be seeded as the value on a miss. Must be
	//combined with an AutoCreateItemOnMissTTLToken. If this value is not set then the default value is 0
	//equivalent to the J<token> flag
	AutoCreateInitialValueOnMissToken *uint64

	//An integer to either add or subtract from the currently stored
	//number. If this is not set then the default delta is 1
	//equivalent to the D<token> flag
	DeltaToken *uint64

	//updates the remaining TTL of an item if hit.
	//equivalent to the T<token> flag
	UpdateTTLToken *int32

	// use if you wish to reduce the amount of data being sent back by memcached
	// equivalent to the q flag
	// note: this will always return an error for commands using this flag
	UseNoReplySemanticsForResponse bool

	//Set this to true if remaining ttl of the item should be included in meta response metadata
	//equivalent to the t flag
	ReturnTTLRemainingSecondsInResponse bool

	//Set this to true if cas token should be included in meta response metadata
	//equivalent to the c flag
	ReturnCasTokenInResponse bool

	//Return new value
	//equivalent to the v flag
	ReturnItemValueInResponse bool

	//Use Increment to increment the value
	//use Decrement to decrement the value
	//if this is not set then the default mode is Increment
	//equivalent to the M<token> flag
	ArithmeticModeToken *MetaArithmeticMode
}

// MetaGetFlags are the flags supported by MetaGet function
type MetaGetFlags struct {

	// Should be true if the key passed to MetaGet is in base64Format.
	// Same as Meta Get b option
	IsKeyBase64 bool

	// Set this to true if cas token should be included in meta get response metadata
	// Same as Meta Get c option
	ReturnCasTokenInResponse bool

	// Set this to true if client flags should be included in meta get response metadata
	// Same as Meta Get f option
	ReturnClientFlagsInResponse bool

	// Set this to true if you want to know if an item is hit before
	// Same as Meta Get h option
	ReturnItemHitInResponse bool

	// Set this to true if key should be included in meta get response metadata
	// Same as Meta Get k option
	ReturnKeyInResponse bool

	// Set this to true if last time since item was accessed in Seconds
	// be included in meta get response metadata
	// Same as Meta Get l option
	ReturnLastAccessedTimeSecondsInResponse bool

	// Set this to true if item size in bytes should be included in meta get response metadata
	// Same as Meta Get s option
	ReturnItemSizeBytesInResponse bool

	// Set this to true if remaining ttl of the item should be included in meta get response metadata
	// Same as Meta Get t option
	ReturnTTLRemainingSecondsInResponse bool

	//Set this to true to access an item without causing it to be "bumped" to the head
	//of the LRU. This also avoids marking an item as being hit or updating its last
	//access time.
	//Same as Meta Get u option
	PreventBumpInLRU bool

	// opaque value, consumes a token and copies back with response
	// Same as Meta Get O option
	OpaqueToken *string

	//If supplied, and meta get does not find the item in cache, it will
	//create a stub item with the key and TTL as supplied.
	//If such an item is created a 	IsReCacheWon in MetaResponseMetadata will be set to true
	//to indicate to a client that they have "won" the right to re cache an item.
	//The automatically created item has 0 bytes of data.
	//Further, requests will see a IsReCacheWonFlagAlreadySent set to true in MetaResponseMetadata
	//to indicate that another client has already received the win flag.
	//Same as Meta Get N option
	VivifyTTLToken *int32

	//If the remaining TTL of an item is
	//below the supplied token, IsReCacheWon in MetaResponseMetadata will be set to true
	//to indicate the client has "won" the right to re cache an item. This allows refreshing an item before it leads to
	//a miss.
	//Same as Meta Get R option
	ReCacheTTLToken *int32

	// updates the remaining TTL of an item if hit.
	// Same as Meta Get T option
	UpdateTTLToken *int32

	//Return  value
	//Same as Meta Get v flag
	ReturnItemValueInResponse bool
}

//MetaResponseMetadata contains response value and metadata from meta commands
//Some of these values can be nil if the corresponding meta flag is not set while calling
//meta functions
type MetaResponseMetadata struct {

	// Cache value. This will be always present for Meta get command
	ReturnItemValue []byte
	// Compare and set token
	CasId *uint64
	// TTL remaining in seconds for the value. -1 means unlimited
	TTLRemainingInSeconds *int32
	// Cache key
	ItemKey *string
	// Client flags
	ClientFlag *uint32
	//Will be true if an item has been hit before
	IsItemHitBefore *bool
	//Cache value size in bytes
	ItemSizeInBytes *uint64
	// Time since item was last accessed in seconds
	TimeInSecondsSinceLastAccessed *uint32
	// Opaque token
	OpaqueToken *string
	//Will be true if client has won the re cache
	//same as W flag from mem cache
	IsReCacheWon bool
	//if this is true then it indicates that a different client
	//is responsible for re caching this item
	//same as Z flag from memcache
	IsReCacheWonFlagAlreadySent bool
	//Will be true if the item is stale
	//same as X flag from memcache
	IsItemStale bool
}

type MetaDebugResponse struct {
	ItemKey        *string
	ExpirationTime *int32
	LastAccessTime *uint32
	CasId          *uint64
	Fetched        bool
	SlabClassId    *uint64
	Size           *uint64
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

//MetaArithmetic Function supports meta arithmetic operation
//Takes key and MetaArithmeticFlags as a param
//Returns MetaResponseMetadata on success
//Errors:
//Returns ErrMalformedKey error if the key is malformed
//Returns ErrCacheMiss error if the key is not found
//Returns ErrNotStored error to indicate that the item was not created as requested after a miss.
//Returns ErrCASConflict error to  indicate that the supplied CAS token does not match the stored item.
func (c *Client) MetaArithmetic(key string, metaArithmeticFlags *MetaArithmeticFlags) (metaResponseMetadata *MetaResponseMetadata, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.metaArithmeticFromAddr(addr,
			key,
			metaArithmeticFlags,
			func(metaArithmeticRes *MetaResponseMetadata) { metaResponseMetadata = metaArithmeticRes })
	})
	return
}

func (c *Client) metaArithmeticFromAddr(addr net.Addr,
	key string, metaArithmeticFlags *MetaArithmeticFlags,
	cb func(response *MetaResponseMetadata)) error {

	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		metaArithmeticFlags := createMetaArithmeticFlagCommands(metaArithmeticFlags)
		if _, err := fmt.Fprintf(rw, "ma %s %s\r\n", key, metaArithmeticFlags); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		if err := parseMetaArithmeticResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil

	})

}

func parseMetaArithmeticResponse(r *bufio.Reader, cb func(metadata *MetaResponseMetadata)) error {
	response, err := r.ReadSlice('\n')

	if err != nil {
		return err
	}

	switch {
	case bytes.HasPrefix(response, metaResultStored) || bytes.HasPrefix(response, metaValue):

		responseComponents := strings.Fields(string(response))
		metaRespMetadata := new(MetaResponseMetadata)
		var responseMetadata []string

		if bytes.HasPrefix(response, metaValue) {
			var size int
			//read value size
			if size, err = strconv.Atoi(responseComponents[1]); err != nil {
				return err
			}

			itemValue := make([]byte, size+2)
			//populate value
			_, err := io.ReadFull(r, itemValue)

			if err != nil {
				return err
			}

			//check if value has a suffix of clrf (i.e. \r\n)
			if !bytes.HasSuffix(itemValue, crlf) {
				return fmt.Errorf("memcache: corrupt meta arithmetic result read")
			}

			metaRespMetadata.ReturnItemValue = itemValue[:size]
			responseMetadata = responseComponents[2:]
		} else {
			responseMetadata = responseComponents[1:]
		}

		if err = parseMetaResponseMetadata(responseMetadata, metaRespMetadata); err != nil {
			return err
		}
		cb(metaRespMetadata)

		return nil

	case bytes.HasPrefix(response, metaResultNotFound):
		return ErrCacheMiss
	case bytes.HasPrefix(response, metaResultNotStored):
		return ErrNotStored
	case bytes.HasPrefix(response, metaResultExists):
		return ErrCASConflict

	default:
		return fmt.Errorf("memcache: unexpected response line from ms: %q", string(response))
	}
}

//MetaGet function returns value for a cached item given a key. It also accepts few MetaGetFlags
//which supports additional functionality.
//ErrCacheMiss is returned if a key is not found
//The key must be at most 250 bytes in length.
//Will return error if metaGetFlags are nil or empty struct
func (c *Client) MetaGet(key string, metaGetFlags *MetaGetFlags) (metaResponseMetadata *MetaResponseMetadata, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.metaGetFromAddr(addr,
			key,
			metaGetFlags,
			func(metaGetRes *MetaResponseMetadata) { metaResponseMetadata = metaGetRes })
	})
	return
}

func (c *Client) metaGetFromAddr(addr net.Addr, key string, metaGetFlags *MetaGetFlags, cb func(response *MetaResponseMetadata)) error {

	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		metaGetCommandFlags := createMetaGetFlagCommands(metaGetFlags)
		if _, err := fmt.Fprintf(rw, "mg %s %s\r\n", key, metaGetCommandFlags); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		if err := parseMetaGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil

	})

}

// function parses meta get response
func parseMetaGetResponse(r *bufio.Reader, cb func(metadata *MetaResponseMetadata)) error {

	response, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	//Check if cache miss has occurred
	case bytes.HasPrefix(response, metaCacheMiss):
		return ErrCacheMiss

	//metaGet command response will be in the format if value is requested in response
	//VA <size> <flags>*\r\n
	//<data block>\r\n
	// if value is not requested then it will be like below
	//HD <flags>*\r\n
	case bytes.HasPrefix(response, metaValue) || bytes.HasPrefix(response, metaResultStored):
		responseComponents := strings.Fields(string(response))
		metaRespMetadata := new(MetaResponseMetadata)
		var responseMetadata []string

		if bytes.HasPrefix(response, metaValue) {
			var size int
			//read value size
			if size, err = strconv.Atoi(responseComponents[1]); err != nil {
				return err
			}

			itemValue := make([]byte, size+2)
			//populate value
			_, err := io.ReadFull(r, itemValue)

			if err != nil {
				return err
			}

			//check if value has a suffix of clrf (i.e. \r\n)
			if !bytes.HasSuffix(itemValue, crlf) {
				return fmt.Errorf("memcache: corrupt meta arithmetic result read")
			}

			metaRespMetadata.ReturnItemValue = itemValue[:size]
			responseMetadata = responseComponents[2:]
		} else {
			responseMetadata = responseComponents[1:]
		}
		if err = parseMetaResponseMetadata(responseMetadata, metaRespMetadata); err != nil {
			return err
		}
		cb(metaRespMetadata)

		return nil
	default:
		return fmt.Errorf("memcache: unexpected response line from ms: %q", string(response))
	}
}

//populates the MetaResponseMetadata based on the response flags
func populateMetaResponseMetadata(metadata string, respMetadata *MetaResponseMetadata) error {
	var err error
	respFlagKey := metadata[0:1]
	respValue := metadata[1:]
	switch respFlagKey {

	case casTokenResponseMetaFlag:
		var casId uint64
		if casId, err = strconv.ParseUint(respValue, 10, 64); err != nil {
			return err
		}
		respMetadata.CasId = &casId

	case ttlResponseMetaFlag:
		var ttl int64

		if ttl, err = strconv.ParseInt(respValue, 10, 32); err != nil {
			return err
		}
		ttl32 := int32(ttl)
		respMetadata.TTLRemainingInSeconds = &ttl32

	case returnKeyAsTokenMetaFlag:
		respMetadata.ItemKey = &respValue

	case clientFlagsResponseMetaFlag:
		var flag *uint32
		if flag, err = convertToUInt32(respValue); err != nil {
			return err
		}
		respMetadata.ClientFlag = flag

	case itemHitResponseMetaFlag:
		hitValue := respValue != "0"
		respMetadata.IsItemHitBefore = &hitValue

	case itemSizeResponseMetaFlag:
		var size uint64
		if size, err = strconv.ParseUint(respValue, 10, 64); err != nil {
			return err
		}
		respMetadata.ItemSizeInBytes = &size

	case lastAccessTimeResponseMetaFlag:
		var lastAccessTime *uint32
		if lastAccessTime, err = convertToUInt32(respValue); err != nil {
			return err
		}
		respMetadata.TimeInSecondsSinceLastAccessed = lastAccessTime

	case opaqueTokenMetaFlag:
		respMetadata.OpaqueToken = &respValue

	case reCacheWonResponseMetaFlag:
		respMetadata.IsReCacheWon = true

	case itemStaleResponseMetaFlag:
		respMetadata.IsItemStale = true

	case reCacheWonAlreadySentResponseMetaFlag:
		respMetadata.IsReCacheWonFlagAlreadySent = true
	}

	return nil
}

func convertToUInt32(num string) (*uint32, error) {

	var err error
	var uInt64Num uint64
	var uInt32Num uint32
	//parse uint always returns uint64 even though bit size is 32.
	if uInt64Num, err = strconv.ParseUint(num, 10, 32); err != nil {
		return nil, err
	}
	//have to convert uint64 to uint32
	uInt32Num = uint32(uInt64Num)
	return &uInt32Num, nil
}

func createMetaArithmeticFlagCommands(flags *MetaArithmeticFlags) string {

	var metaFlagCommands strings.Builder

	if flags != nil {
		if flags.UpdateTTLToken != nil {
			updateTTLTokenFlag := updateTTLTokenMetaFlag + strconv.FormatInt(int64(*flags.UpdateTTLToken), 10)
			metaFlagCommands.WriteString(updateTTLTokenFlag + " ")
		}

		if flags.IsKeyBase64 {
			metaFlagCommands.WriteString(base64MetaFlag + " ")
		}

		if flags.ReturnCasTokenInResponse {
			metaFlagCommands.WriteString(casTokenResponseMetaFlag + " ")
		}

		if flags.CompareCasTokenToUpdateValue != nil {
			casTokenFlag := compareCasValueTokenMetaFlag + strconv.FormatUint(*flags.CompareCasTokenToUpdateValue, 10)
			metaFlagCommands.WriteString(casTokenFlag + " ")
		}

		if flags.AutoCreateItemOnMissTTLToken != nil {
			autoCreateOnMissTokenFlag := autoCreateItemOnMissTokenMetaFlag +
				strconv.FormatInt(int64(*flags.AutoCreateItemOnMissTTLToken), 10)
			metaFlagCommands.WriteString(autoCreateOnMissTokenFlag + " ")
		}

		if flags.AutoCreateInitialValueOnMissToken != nil {
			initialValueTokenFlag := initialValueTokenMetaFlag +
				strconv.FormatUint(*flags.AutoCreateInitialValueOnMissToken, 10)
			metaFlagCommands.WriteString(initialValueTokenFlag + " ")
		}

		if flags.DeltaToken != nil {
			deltaTokenFlag := deltaTokenMetaFlag +
				strconv.FormatUint(*flags.DeltaToken, 10)
			metaFlagCommands.WriteString(deltaTokenFlag + " ")
		}

		if flags.ArithmeticModeToken != nil {
			modeTokenFlags := modeTokenMetaFlag + string(*flags.ArithmeticModeToken)
			metaFlagCommands.WriteString(modeTokenFlags + " ")
		}

		if flags.UseNoReplySemanticsForResponse {
			metaFlagCommands.WriteString(noReplySemanticsMetaFlag + " ")
		}

		if flags.ReturnTTLRemainingSecondsInResponse {
			metaFlagCommands.WriteString(ttlResponseMetaFlag + " ")
		}

		if flags.ReturnItemValueInResponse {
			metaFlagCommands.WriteString(itemValueMetaFlag + " ")
		}

	}

	return strings.TrimSuffix(metaFlagCommands.String(), " ")
}

//creates the necessary meta get flag commands from the MetaGetFlags struct
func createMetaGetFlagCommands(metaFlags *MetaGetFlags) string {

	var metaFlagCommands strings.Builder
	if metaFlags != nil {
		if metaFlags.ReturnItemValueInResponse {
			metaFlagCommands.WriteString(itemValueMetaFlag + " ")
		}

		if metaFlags.UpdateTTLToken != nil {
			updateTTLTokenFlag := updateTTLTokenMetaFlag + strconv.FormatInt(int64(*metaFlags.UpdateTTLToken), 10)
			metaFlagCommands.WriteString(updateTTLTokenFlag + " ")
		}

		if metaFlags.IsKeyBase64 {
			metaFlagCommands.WriteString(base64MetaFlag + " ")
		}

		if metaFlags.ReturnCasTokenInResponse {
			metaFlagCommands.WriteString(casTokenResponseMetaFlag + " ")
		}

		if metaFlags.ReturnTTLRemainingSecondsInResponse {
			metaFlagCommands.WriteString(ttlResponseMetaFlag + " ")
		}

		if metaFlags.ReturnKeyInResponse {
			metaFlagCommands.WriteString(returnKeyAsTokenMetaFlag + " ")
		}

		if metaFlags.ReturnClientFlagsInResponse {
			metaFlagCommands.WriteString(clientFlagsResponseMetaFlag + " ")
		}

		if metaFlags.ReturnItemHitInResponse {
			metaFlagCommands.WriteString(itemHitResponseMetaFlag + " ")
		}

		if metaFlags.ReturnItemSizeBytesInResponse {
			metaFlagCommands.WriteString(itemSizeResponseMetaFlag + " ")
		}

		if metaFlags.ReturnLastAccessedTimeSecondsInResponse {
			metaFlagCommands.WriteString(lastAccessTimeResponseMetaFlag + " ")
		}

		if metaFlags.OpaqueToken != nil {
			opaqueToken := opaqueTokenMetaFlag + *metaFlags.OpaqueToken
			metaFlagCommands.WriteString(opaqueToken + " ")
		}

		if metaFlags.VivifyTTLToken != nil {
			vivifyToken := vivifyOnMissTokenMetaFlag + strconv.FormatInt(int64(*metaFlags.VivifyTTLToken), 10)
			metaFlagCommands.WriteString(vivifyToken + " ")
		}

		if metaFlags.ReCacheTTLToken != nil {
			reCacheTTLToken := reCacheTTLTokenMetaFlag + strconv.FormatInt(int64(*metaFlags.ReCacheTTLToken), 10)
			metaFlagCommands.WriteString(reCacheTTLToken + " ")
		}

		if metaFlags.PreventBumpInLRU {
			metaFlagCommands.WriteString(dontBumpItemInLruMetaFlag + " ")
		}
	}

	return strings.TrimSuffix(metaFlagCommands.String(), " ")
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
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	if bytes.Count(line, space) == 3 {
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.onItem(item, (*Client).set)
}

func (c *Client) set(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "set", item)
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

type MetaDebugItem struct {
	Key         string
	IsKeyBase64 bool
}

type MetaSetItem struct {
	Key   string
	Flags MetaSetFlags
	Value []byte
}
type MetaDeleteItem struct {
	Key   string
	Flags MetaDeleteFlags
}

type MetaSetFlags struct {
	// use if the key provided is base 64 encoded
	// equivalent to the b flag
	IsKeyBase64 bool

	// use if you wish to see the cas token as a part of the response
	// equivalent to the c flag
	ReturnCasTokenInResponse bool

	// use if you provide a cas token with CompareCasTokenToUpdateValue attribute and it is older than the item's CAS
	// note: only functional when combined with the CompareCasTokenToUpdateValue attribute
	// equivalent to the I flag
	Invalidate bool

	// use if you want to get the key as a part of the response
	// equivalent to the k flag
	ReturnKeyInResponse bool

	// use if you wish to reduce the amount of data being sent back by memcached
	// note: this will always return an error for commands using this flag
	// equivalent to the q flag
	UseNoReplySemanticsForResponse bool

	// use if you want to switch modes
	// E: "add" command. LRU bump and return NS if item exists, else add
	// A: "append" command. If item exists, append the new value to its data
	// P: "prepend" command. If item exists, prepend the new value to its data
	// R: "replace" command. Set only if item exists, replace its value
	// S: "set" command. The default mode, added for completeness
	// equivalent to the M<token> flag
	SetModeToken *MetaSetMode

	// use if only want to store a value if the supplied token matches the current CAS token of the item
	// equivalent to the C<token> flag
	CompareCasTokenToUpdateValue *uint64

	// use if you want to set client flags to a token
	// equivalent to the F<token> flag
	ClientFlagToken *uint32

	// use if you want to consume a token and copy back with a response
	// equivalent to the O<token> flag
	OpaqueToken *string

	// use if you want to set the TTL for the item
	// equivalent to the T<token> flag
	UpdateTTLToken *int32
}

type MetaDeleteFlags struct {
	// use if the key provided is base 64 encoded
	// equivalent to the b flag
	IsKeyBase64 bool

	// instead of removing an item, it will give the item a new CAS value and mark it as stale so the next metaget
	// will be supplied an 'X' flag to show that the data is stale and needs to be recached
	// equivalent to the I flag
	Invalidate bool

	// use if you want to get the key as a part of the response
	// equivalent to the k flag
	ReturnKeyInResponse bool

	// use if you wish to reduce the amount of data being sent back by memcached
	// note: this will always return an error for commands using this flag
	// equivalent to the q flag
	UseNoReplySemanticsForResponse bool

	// use if only want to store a value if the supplied token matches the current CAS token of the item
	// equivalent to the C<token> flag
	CompareCasTokenToUpdateValue *uint64

	// use if you want to consume a token and copy back with a response
	// equivalent to the O<token> flag
	OpaqueToken *string

	// use if you want to set the TTL for the item
	// note: only works when paired with the Invalidate attribute
	// equivalent to the T<token> flag
	UpdateTTLToken *int32
}

/*
This function provides functionality in Golang for the meta set command. The meta set command is a more flexible
approach to setting values in memcached- instead of having multiple methods each do different things, you can consolidate
everything you wish to do in one line with the meta command and its array of flags.

Arguments
@metaItem: encapsulates the key of the item, its new value, and all flags to apply to the meta set command

Return values
@metaDataResponse: encapsulates all values returned as a result of the flags which return a value applied to the meta set command
@err: error that can be raised as a result of the operation- errors include: error because value wasn't stored, error because of cache miss,
error because the item alerady exists (occurs with certain flags), I/O errors, malformed key error, and generic error for an unknown response
from memcached
*/
func (c *Client) MetaSet(metaItem *MetaSetItem) (metaDataResponse *MetaResponseMetadata, err error) {
	err = c.onMetaItem(metaItem, (*Client).processMetaSet, func(metaData *MetaResponseMetadata) { metaDataResponse = metaData })
	return
}

/*
This function provides functionality in Golang for the meta delete command. The meta delete command is a more flexible
approach to deleting values in memcached- instead of having multiple methods each do different things, you can consolidate
everything you wish to do in one line with the meta command and its array of flags.

Arguments
@metaItem: encapsulates the key of the item and all flags to apply to the meta delete command

Return values
@metaDataResponse: encapsulates all values returned as a result of the flags which return a value applied to the meta delete command
@err: error that can be raised as a result of the operation- errors include: malformed key, error establishing server connection, cache miss,
CAS token conflict, and potential client/server errors
*/
func (c *Client) MetaDelete(metaItem *MetaDeleteItem) (metaDataResponse *MetaResponseMetadata, err error) {
	err = c.withKeyRw(metaItem.Key, func(rw *bufio.ReadWriter) error {
		return c.processMetaDelete(rw, c.parseFlagsForMetaDelete(metaItem), func(metaData *MetaResponseMetadata) { metaDataResponse = metaData })
	})
	return
}

/*
This function provides functionality in Golang for the meta debug command. The meta debug command is a human readable dump of all
available internal metadata of an item, minus its value.

Arguments
@metaItem: encapsulates the key of the item and support for a base-64 key encoded flag

Return values
@metaDebugResponse: encapsulates all values returned as a result of the meta debug command
@err: error that can be raised as a result of the operation- errors include: malformed key, error establishing server connection, cache miss,
CAS token conflict, and potential client/server errors
*/
func (c *Client) MetaDebug(metaItem *MetaDebugItem) (metaDebugResponse *MetaDebugResponse, err error) {
	err = c.withKeyRw(metaItem.Key, func(rw *bufio.ReadWriter) error {
		return c.processMetaDebug(rw, c.parseMetaDebugItem(metaItem), func(response *MetaDebugResponse) { metaDebugResponse = response })
	})
	return
}

func (c *Client) parseMetaDebugItem(metaItem *MetaDebugItem) string {
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("me %s", metaItem.Key))
	if metaItem.IsKeyBase64 {
		commandBuilder.WriteString(" b")
	}
	commandBuilder.WriteString(string(crlf))
	return commandBuilder.String()
}

func (c *Client) processMetaDebug(rw *bufio.ReadWriter, command string, cb func(*MetaDebugResponse)) error {
	response, err := writeReadLine(rw, command)
	if err != nil {
		return err
	}
	if bytes.HasPrefix(response, metaDebugSuccess) {
		metaResponseMetadataComponents := strings.Fields(string(response))[2:]
		metaDebugResponse := new(MetaDebugResponse)
		metaDebugResponse.ItemKey = &metaResponseMetadataComponents[1]
		if err = c.parseMetaDebugResponseMetadata(metaResponseMetadataComponents, metaDebugResponse); err != nil {
			return err
		}
		cb(metaDebugResponse)
		return nil
	} else if bytes.HasPrefix(response, metaCacheMiss) {
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from me: %q", string(response))
}

func (c *Client) parseMetaDebugResponseMetadata(metadata []string, response *MetaDebugResponse) error {
	for _, component := range metadata {
		if err := c.populateMetaDebugResponse(component, response); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) populateMetaDebugResponse(component string, response *MetaDebugResponse) error {
	// response after me <key> is in format of k=v
	keyValueComponents := strings.Split(component, "=")
	key := keyValueComponents[0]
	value := keyValueComponents[1]
	var err error
	switch key {
	case expirationTimeKey:
		var expirationTime64 int64
		if expirationTime64, err = strconv.ParseInt(value, 10, 32); err != nil {
			return err
		}
		timeToSave := int32(expirationTime64)
		response.ExpirationTime = &timeToSave
	case lastAccessTimeKey:
		var lastAccessedTime64 int64
		if lastAccessedTime64, err = strconv.ParseInt(value, 10, 32); err != nil {
			return err
		}
		timeToSave := uint32(lastAccessedTime64)
		response.LastAccessTime = &timeToSave
	case casIdKey:
		var casId uint64
		if casId, err = strconv.ParseUint(value, 10, 64); err != nil {
			return err
		}
		response.CasId = &casId
	case fetchKey:
		response.Fetched = value == "yes"
	case slabClassIdKey:
		var slabId uint64
		if slabId, err = strconv.ParseUint(value, 10, 64); err != nil {
			return err
		}
		response.SlabClassId = &slabId
	case sizeKey:
		var size uint64
		if size, err = strconv.ParseUint(value, 10, 64); err != nil {
			return err
		}
		response.Size = &size
	}
	return nil
}

func (c *Client) onMetaItem(item *MetaSetItem, fn func(*Client, *bufio.ReadWriter, *MetaSetItem, func(metaData *MetaResponseMetadata)) error, cb func(metaData *MetaResponseMetadata)) error {
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	if err = fn(c, cn.rw, item, cb); err != nil {
		return err
	}
	return nil
}

func (c *Client) processMetaSet(rw *bufio.ReadWriter, item *MetaSetItem, cb func(*MetaResponseMetadata)) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}

	command := c.parseFlagsForMetaSet(item)

	var err error
	_, err = fmt.Fprintf(rw, command)

	if err != nil {
		return nil
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
	response, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.HasPrefix(response, metaResultStored):
		// the first two characters are being processed in this switch-case block, the other cases are all errors
		// so we don't need to save them into the meta data object
		responseMetadataComponents := strings.Fields(string(response))[1:]
		metaResponseMetadata := new(MetaResponseMetadata)
		if err = parseMetaResponseMetadata(responseMetadataComponents, metaResponseMetadata); err != nil {
			return err
		}
		cb(metaResponseMetadata)
		return nil
	case bytes.HasPrefix(response, metaResultNotStored):
		return ErrNotStored
	case bytes.HasPrefix(response, metaResultExists):
		return ErrCASConflict
	case bytes.HasPrefix(response, metaResultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from ms: %q", string(response))
}

func (c *Client) processMetaDelete(rw *bufio.ReadWriter, command string, cb func(*MetaResponseMetadata)) error {
	response, err := writeReadLine(rw, command)
	if err != nil {
		return err
	}
	switch {
	case bytes.HasPrefix(response, metaResultDeleted):
		metaResponseMetadataComponents := strings.Fields(string(response))[1:]
		metaResponseMetadata := new(MetaResponseMetadata)
		if err = parseMetaResponseMetadata(metaResponseMetadataComponents, metaResponseMetadata); err != nil {
			return err
		}
		cb(metaResponseMetadata)
		return nil
	case bytes.HasPrefix(response, metaResultNotFound):
		return ErrCacheMiss
	case bytes.HasPrefix(response, metaResultExists):
		return ErrCASConflict
	}
	return fmt.Errorf("memcache: unexpected response line from md: %q", string(response))
}

//function reads the  response from meta commands and returns a struct MetaResponseMetadata
//response of meta commands contain flags which are single characters.
// For ex if MetaGetFlags.ReturnTTLRemainingSecondsInResponse is set to true then response
//will contain t300. Here 300 is the amount of TTL remaining in Seconds
func parseMetaResponseMetadata(metaResponseMetadata []string, respMetadata *MetaResponseMetadata) error {
	for _, metadata := range metaResponseMetadata {
		if err := populateMetaResponseMetadata(metadata, respMetadata); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) parseFlagsForMetaSet(metaItem *MetaSetItem) string {
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("ms %s %d", metaItem.Key, len(metaItem.Value)))

	itemFlags := metaItem.Flags
	if itemFlags.IsKeyBase64 {
		commandBuilder.WriteString(fmt.Sprintf(" %s", base64MetaFlag))
	}
	if itemFlags.ReturnCasTokenInResponse {
		commandBuilder.WriteString(fmt.Sprintf(" %s", casTokenResponseMetaFlag))
	}
	if itemFlags.Invalidate {
		commandBuilder.WriteString(fmt.Sprintf(" %s", invalidateMetaFlag))
	}
	if itemFlags.ReturnKeyInResponse {
		commandBuilder.WriteString(fmt.Sprintf(" %s", returnKeyAsTokenMetaFlag))
	}
	if itemFlags.UseNoReplySemanticsForResponse {
		commandBuilder.WriteString(fmt.Sprintf(" %s", noReplySemanticsMetaFlag))
	}
	if itemFlags.CompareCasTokenToUpdateValue != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%d", compareCasValueTokenMetaFlag, *itemFlags.CompareCasTokenToUpdateValue))
	}
	if itemFlags.ClientFlagToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%d", setClientFlagsToTokenMetaFlag, *itemFlags.ClientFlagToken))
	}
	if itemFlags.OpaqueToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%s", opaqueTokenMetaFlag, *itemFlags.OpaqueToken))
	}
	if itemFlags.UpdateTTLToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%d", updateTTLTokenMetaFlag, *itemFlags.UpdateTTLToken))
	}
	if itemFlags.SetModeToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%s", modeTokenMetaFlag, *itemFlags.SetModeToken))
	}
	commandBuilder.WriteString(string(crlf))

	return commandBuilder.String()
}

func (c *Client) parseFlagsForMetaDelete(metaItem *MetaDeleteItem) string {
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("md %s", metaItem.Key))

	itemFlags := metaItem.Flags
	if itemFlags.IsKeyBase64 {
		commandBuilder.WriteString(fmt.Sprintf(" %s", base64MetaFlag))
	}
	if itemFlags.Invalidate {
		commandBuilder.WriteString(fmt.Sprintf(" %s", invalidateMetaFlag))
	}
	if itemFlags.ReturnKeyInResponse {
		commandBuilder.WriteString(fmt.Sprintf(" %s", returnKeyAsTokenMetaFlag))
	}
	if itemFlags.UseNoReplySemanticsForResponse {
		commandBuilder.WriteString(fmt.Sprintf(" %s", noReplySemanticsMetaFlag))
	}
	if itemFlags.CompareCasTokenToUpdateValue != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%d", compareCasValueTokenMetaFlag, *itemFlags.CompareCasTokenToUpdateValue))
	}
	if itemFlags.OpaqueToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%s", opaqueTokenMetaFlag, *itemFlags.OpaqueToken))
	}
	if itemFlags.UpdateTTLToken != nil {
		commandBuilder.WriteString(fmt.Sprintf(" %s%d", updateTTLTokenMetaFlag, *itemFlags.UpdateTTLToken))
	}
	commandBuilder.WriteString(string(crlf))

	return commandBuilder.String()
}
