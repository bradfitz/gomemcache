<!--
	Copyright 2009 The Go Authors. All rights reserved.
	Use of this source code is governed by a BSD-style
	license that can be found in the LICENSE file.
-->

	<!-- PackageName is printed as title by the top-level template -->
		<p><code>import "."</code></p>
	<p>
Package memcache provides a client for the memcached cache server.
</p>

			<p>
			<h4>Package files</h4>
			<span style="font-size:90%">
				<a href="/">memcache.go</a>
				<a href="/">selector.go</a>
			</span>
			</p>
		<h2 id="Constants">Constants</h2>
			<p>
DefaultTimeoutNanos is the default socket read/write timeout, in nanoseconds.
</p>

			<pre>const DefaultTimeoutNanos = 100e6 <span class="comment">// 100 ms</span>
</pre>
		<h2 id="Variables">Variables</h2>
			
			<pre>var (
    <span class="comment">// ErrCacheMiss means that a Get failed because the item wasn&#39;t present.</span>
    ErrCacheMiss = os.NewError(&#34;memcache: cache miss&#34;)

    <span class="comment">// ErrCASConflict means that a CompareAndSwap call failed due to the</span>
    <span class="comment">// cached value being modified between the Get and the CompareAndSwap.</span>
    <span class="comment">// If the cached value was simply evicted rather than replaced,</span>
    <span class="comment">// ErrNotStored will be returned instead.</span>
    ErrCASConflict = os.NewError(&#34;memcache: compare-and-swap conflict&#34;)

    <span class="comment">// ErrNotStored means that a conditional write operation (i.e. Add or</span>
    <span class="comment">// CompareAndSwap) failed because the condition was not satisfied.</span>
    ErrNotStored = os.NewError(&#34;memcache: item not stored&#34;)

    <span class="comment">// ErrServer means that a server error occurred.</span>
    ErrServerError = os.NewError(&#34;memcache: server error&#34;)

    <span class="comment">// ErrNoStats means that no statistics were available.</span>
    ErrNoStats = os.NewError(&#34;memcache: no statistics available&#34;)

    <span class="comment">// ErrMalformedKey is returned when an invalid key is used.</span>
    <span class="comment">// Keys must be at maximum 250 bytes long, ASCII, and not</span>
    <span class="comment">// contain whitespace or control characters.</span>
    ErrMalformedKey = os.NewError(&#34;malformed: key is too long or contains invalid characters&#34;)

    <span class="comment">// ErrNoServers is returned when no servers are configured or available.</span>
    ErrNoServers = os.NewError(&#34;memcache: no servers configured or available&#34;)
)</pre>
			<h2 id="Client">type <a href="/?s=3851:4072#L115">Client</a></h2>
			<p>
Client is a memcache client.
It is safe for unlocked use by multiple concurrent goroutines.
</p>

			<p><pre>type Client struct {
    <span class="comment">// TimeoutNanos specifies the socket read/write timeout.</span>
    <span class="comment">// If zero, DefaultTimeoutNanos is used.</span>
    TimeoutNanos int64
    <span class="comment">// contains filtered or unexported fields</span>
}</pre></p>
				<h3 id="Client.New">func <a href="/?s=3478:3512#L102">New</a></h3>
				<p><code>func New(server ...string) *Client</code></p>
				<p>
New returns a memcache client using the provided server(s)
with equal weight. If a server is listed multiple times,
it gets a proportional amount of weight.
</p>

				<h3 id="Client.NewFromSelector">func <a href="/?s=3670:3717#L109">NewFromSelector</a></h3>
				<p><code>func NewFromSelector(ss ServerSelector) *Client</code></p>
				<p>
NewFromSelector returns a new Client using the provided ServerSelector.
</p>

				<h3 id="Client.Add">func (*Client) <a href="/?s=10366:10407#L379">Add</a></h3>
				<p><code>func (c *Client) Add(item *Item) os.Error</code></p>
				<p>
Add writes the given item, if no value already exists for its
key. ErrNotStored is returned if that condition is not met.
</p>

				<h3 id="Client.CompareAndSwap">func (*Client) <a href="/?s=10977:11029#L394">CompareAndSwap</a></h3>
				<p><code>func (c *Client) CompareAndSwap(item *Item) os.Error</code></p>
				<p>
CompareAndSwap writes the given item that was previously returned
by Get, if the value was neither modified or evicted between the
Get and the CompareAndSwap calls. The item&#39;s Key should not change
between calls but all other item fields may differ. ErrCASConflict
is returned if the value was modified in between the
calls. ErrNotStored is returned if the value was evicted in between
the calls.
</p>

				<h3 id="Client.Delete">func (*Client) <a href="/?s=13002:13046#L470">Delete</a></h3>
				<p><code>func (c *Client) Delete(key string) os.Error</code></p>
				<p>
Delete deletes the item with the provided key. The error ErrCacheMiss is
returned if the item didn&#39;t already exist in the cache.
</p>

				<h3 id="Client.DeleteLock">func (*Client) <a href="/?s=13351:13412#L478">DeleteLock</a></h3>
				<p><code>func (c *Client) DeleteLock(key string, seconds int) os.Error</code></p>
				<p>
Delete deletes the item with the provided key, also instructing the
server to not permit an &#34;add&#34; or &#34;replace&#34; commands to work on the
key for the given duration (in seconds). The error ErrCacheMiss is
returned if the item didn&#39;t already exist in the cache.
</p>

				<h3 id="Client.Get">func (*Client) <a href="/?s=6852:6911#L242">Get</a></h3>
				<p><code>func (c *Client) Get(key string) (item *Item, err os.Error)</code></p>
				<p>
Get gets the item for the given key. ErrCacheMiss is returned for a
memcache cache miss. The key must be at most 250 bytes in length.
</p>

				<h3 id="Client.GetMulti">func (*Client) <a href="/?s=8411:8480#L297">GetMulti</a></h3>
				<p><code>func (c *Client) GetMulti(keys []string) (map[string]*Item, os.Error)</code></p>
				<p>
GetMulti is a batch version of Get. The returned map from keys to
items may have fewer elements than the input slice, due to memcache
cache misses. Each key must be at most 250 bytes in length.
If no error is returned, the returned map will also be non-nil.
</p>

				<h3 id="Client.Set">func (*Client) <a href="/?s=10045:10086#L369">Set</a></h3>
				<p><code>func (c *Client) Set(item *Item) os.Error</code></p>
				<p>
Set writes the given item, unconditionally.
</p>

			<h2 id="Item">type <a href="/?s=4136:4692#L127">Item</a></h2>
			<p>
Item is an item to be got or stored in a memcached server.
</p>

			<p><pre>type Item struct {
    <span class="comment">// Key is the Item&#39;s key (250 bytes maximum).</span>
    Key string

    <span class="comment">// Value is the Item&#39;s value.</span>
    Value []byte

    <span class="comment">// Object is the Item&#39;s value for use with a Codec.</span>
    Object interface{}

    <span class="comment">// Flags are server-opaque flags whose semantics are entirely up to the</span>
    <span class="comment">// App Engine app.</span>
    Flags uint32

    <span class="comment">// Expiration is the cache expiration time, in seconds: either a relative</span>
    <span class="comment">// time from now (up to 1 month), or an absolute Unix epoch time.</span>
    <span class="comment">// Zero means the Item has no expiration time.</span>
    Expiration int32
    <span class="comment">// contains filtered or unexported fields</span>
}</pre></p>
			<h2 id="ServerList">type <a href="/?s=1023:1087#L27">ServerList</a></h2>
			<p>
ServerList is a simple ServerSelector. Its zero value is usable.
</p>

			<p><pre>type ServerList struct {
    <span class="comment">// contains filtered or unexported fields</span>
}</pre></p>
				<h3 id="ServerList.PickServer">func (*ServerList) <a href="/?s=1793:1858#L57">PickServer</a></h3>
				<p><code>func (ss *ServerList) PickServer(key string) (net.Addr, os.Error)</code></p>
				
				<h3 id="ServerList.SetServers">func (*ServerList) <a href="/?s=1473:1533#L41">SetServers</a></h3>
				<p><code>func (ss *ServerList) SetServers(servers ...string) os.Error</code></p>
				<p>
SetServers changes a ServerList&#39;s set of servers at runtime and is
threadsafe.
</p>
<p>
Each server is given equal weight. A server is given more weight
if it&#39;s listed multiple times.
</p>
<p>
SetServers returns an error if any of the server names fail to
resolve. No attempt is made to connect to the server. If any error
is returned, no changes are made to the ServerList.
</p>

			<h2 id="ServerSelector">type <a href="/?s=788:953#L20">ServerSelector</a></h2>
			<p>
ServerSelector is the interface that selects a memcache server
as a function of the item&#39;s key.
</p>
<p>
All ServerSelector implementations must be threadsafe.
</p>

			<p><pre>type ServerSelector interface {
    <span class="comment">// PickServer returns the server address that a given item</span>
    <span class="comment">// should be shared onto.</span>
    PickServer(key string) (net.Addr, os.Error)
}</pre></p>
	<h2>Other packages</h2>
	<p>
	<a href="?p=main">main</a><br />
	</p>
