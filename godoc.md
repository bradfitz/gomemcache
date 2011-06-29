## Package memcache

	import "github.com/bradfitz/gomemcache"

Package memcache provides a client for the memcached cache server.

## Constants

  DefaultTimeoutNanos is the default socket read/write timeout, in nanoseconds.

  const DefaultTimeoutNanos = 100e6 // 100 ms

## Variables
  
  var (
    // ErrCacheMiss means that a Get failed because the item wasn't present.
    ErrCacheMiss = os.NewError("memcache: cache miss")

    // ErrCASConflict means that a CompareAndSwap call failed due to the
    // cached value being modified between the Get and the CompareAndSwap.
    // If the cached value was simply evicted rather than replaced,
    // ErrNotStored will be returned instead.
    ErrCASConflict = os.NewError("memcache: compare-and-swap conflict")

    // ErrNotStored means that a conditional write operation (i.e. Add or
    // CompareAndSwap) failed because the condition was not satisfied.
    ErrNotStored = os.NewError("memcache: item not stored")

    // ErrServer means that a server error occurred.
    ErrServerError = os.NewError("memcache: server error")

    // ErrNoStats means that no statistics were available.
    ErrNoStats = os.NewError("memcache: no statistics available")

    // ErrMalformedKey is returned when an invalid key is used.
    // Keys must be at maximum 250 bytes long, ASCII, and not
    // contain whitespace or control characters.
    ErrMalformedKey = os.NewError("malformed: key is too long or contains invalid characters")

    // ErrNoServers is returned when no servers are configured or available.
    ErrNoServers = os.NewError("memcache: no servers configured or available")
)


### type Client

Client is a memcache client.
It is safe for unlocked use by multiple concurrent goroutines.

  type Client struct {
    // TimeoutNanos specifies the socket read/write timeout.
    // If zero, DefaultTimeoutNanos is used.
    TimeoutNanos int64
    // contains filtered or unexported fields
}
				<h3 id="Client.New">func <a href="/src/pkg/github.com/bradfitz/gomemcache/memcache.go?s=3478:3512#L102">New</a></h3>
				<p><code>func New(server ...string) *Client</code></p>
				<p>
New returns a memcache client using the provided server(s)
with equal weight. If a server is listed multiple times,
it gets a proportional amount of weight.
</p>

				<h3 id="Client.NewFromSelector">func <a href="/src/pkg/github.com/bradfitz/gomemcache/memcache.go?s=3670:3717#L109">NewFromSelector</a></h3>
				<p><code>func NewFromSelector(ss ServerSelector) *Client</code></p>
				<p>
NewFromSelector returns a new Client using the provided ServerSelector.
</p>


#### func (*Client) Add

  func (c *Client) Add(item *Item) os.Error
  Add writes the given item, if no value already exists for its
key. ErrNotStored is returned if that condition is not met.

#### func (*Client) CompareAndSwap

  func (c *Client) CompareAndSwap(item *Item) os.Error
  CompareAndSwap writes the given item that was previously returned
by Get, if the value was neither modified or evicted between the
Get and the CompareAndSwap calls. The item's Key should not change
between calls but all other item fields may differ. ErrCASConflict
is returned if the value was modified in between the
calls. ErrNotStored is returned if the value was evicted in between
the calls.

#### func (*Client) Delete

  func (c *Client) Delete(key string) os.Error
  Delete deletes the item with the provided key. The error ErrCacheMiss is
returned if the item didn't already exist in the cache.

#### func (*Client) DeleteLock

  func (c *Client) DeleteLock(key string, seconds int) os.Error
  Delete deletes the item with the provided key, also instructing the
server to not permit an "add" or "replace" commands to work on the
key for the given duration (in seconds). The error ErrCacheMiss is
returned if the item didn't already exist in the cache.

#### func (*Client) Get

  func (c *Client) Get(key string) (item *Item, err os.Error)
  Get gets the item for the given key. ErrCacheMiss is returned for a
memcache cache miss. The key must be at most 250 bytes in length.

#### func (*Client) GetMulti

  func (c *Client) GetMulti(keys []string) (map[string]*Item, os.Error)
  GetMulti is a batch version of Get. The returned map from keys to
items may have fewer elements than the input slice, due to memcache
cache misses. Each key must be at most 250 bytes in length.
If no error is returned, the returned map will also be non-nil.

#### func (*Client) Set

  func (c *Client) Set(item *Item) os.Error
  Set writes the given item, unconditionally.

### type Item

Item is an item to be got or stored in a memcached server.

  type Item struct {
    // Key is the Item's key (250 bytes maximum).
    Key string

    // Value is the Item's value.
    Value []byte

    // Object is the Item's value for use with a Codec.
    Object interface{}

    // Flags are server-opaque flags whose semantics are entirely up to the
    // App Engine app.
    Flags uint32

    // Expiration is the cache expiration time, in seconds: either a relative
    // time from now (up to 1 month), or an absolute Unix epoch time.
    // Zero means the Item has no expiration time.
    Expiration int32
    // contains filtered or unexported fields
}

### type ServerList

ServerList is a simple ServerSelector. Its zero value is usable.

  type ServerList struct {
    // contains filtered or unexported fields
}

#### func (*ServerList) PickServer

  func (ss *ServerList) PickServer(key string) (net.Addr, os.Error)
  
#### func (*ServerList) SetServers

  func (ss *ServerList) SetServers(servers ...string) os.Error
  SetServers changes a ServerList's set of servers at runtime and is
threadsafe.

Each server is given equal weight. A server is given more weight
if it's listed multiple times.

SetServers returns an error if any of the server names fail to
resolve. No attempt is made to connect to the server. If any error
is returned, no changes are made to the ServerList.

### type ServerSelector

ServerSelector is the interface that selects a memcache server
as a function of the item's key.

All ServerSelector implementations must be threadsafe.

  type ServerSelector interface {
    // PickServer returns the server address that a given item
    // should be shared onto.
    PickServer(key string) (net.Addr, os.Error)
}


