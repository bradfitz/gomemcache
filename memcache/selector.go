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

package memcache

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// ServerSelector is the interface that selects a memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be safe for concurrent use
// by multiple goroutines.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, error)
	// Each iterates func over servers, returning the first non-nil error
	Each(func(net.Addr) error) error
	// OnResult informs the selector of the result of an operation - err is nil on success
	OnResult(addr net.Addr, err error)
}

const maxRetryWait = time.Second * 10
const startingWait = time.Millisecond * 10

// retryState drives the per connection state-machine through the retry process
type retryState string

const (
	// kept unavailable until ready for retry
	retryWait retryState = "RetryWait"
	// ready to be picked
	retryReady retryState = "RetryReady"
	// address is having a single operation run against it to see if sever has come back online
	retryRunning retryState = "RetryRunning"
)

type waitState struct {
	addr net.Addr
	wait time.Duration
	retry retryState
}

// serversWithBreaker is a ServerSelector with circuit-breaking
type serversWithBreaker struct {
	// protects the available and states fields
	mu sync.Mutex

	// addrs - not mutated after construction
	addrs []net.Addr

	// optimization: the presence of this field allows us to avoid allocations in PickServer, while preserving the
	// ability to load balance with duplicate addrs
	available []net.Addr
	// records the waitState of servers that have had connection issues. This is
	// used in filterAvailable to decide which servers are currently considered
	// available for connection
	states map[net.Addr]waitState
}

// NewSelectorWithBreaker will resolve all addrs once, and return
// a selector list that takes addresses out of the list on error.
// Addresses are retried with an exponential backoff.
func NewSelectorWithBreaker(addrs []string) (ServerSelector, error) {
	ss := new(serversWithBreaker)
	if err := ss.resolveServers(addrs...); err != nil {
		return nil, err
	}
	return ss, nil
}

var _ ServerSelector = &serversWithBreaker{}

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

// resolveServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server.
func (ss *serversWithBreaker) resolveServers(servers ...string) error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(addr)
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(tcpaddr)
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr
	ss.available = make([]net.Addr, 0, len(ss.addrs))
	ss.filterAvailable()
	return nil
}

// Each iterates over each server, regardless of current availability
func (ss *serversWithBreaker) Each(f func(net.Addr) error) error {
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

func (ss *serversWithBreaker) PickServer(key string) (picked net.Addr, err error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// if we pick a connection for retry, remove it from the available pool till we
	// hear the result
	defer (func() {
		if err != nil {
			return
		}
		// remove the address from available roster again to avoid multiple retries
		if ws, ok := ss.states[picked]; ok && ws.retry == retryReady {
			ws.retry = retryRunning
			ss.setState(picked, ws)
		}
	})()

	if len(ss.available) == 0 {
		return nil, ErrNoServers
	}
	if len(ss.available) == 1 {
		return ss.available[0], nil
	}
	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n])
	keyBufPool.Put(bufp)

	return ss.available[cs%uint32(len(ss.available))], nil
}

func (ss *serversWithBreaker) OnResult(addr net.Addr, err error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if err == nil || !isConnectionError(err) {
		// server is considered available once we successfully
		// communicate
		if _, ok := ss.states[addr]; ok {
			ss.deleteState(addr)
		}
		return
	}

	ws := waitState{
		addr:  addr,
		wait:  startingWait,
		retry: retryWait,
	}
	if st, ok := ss.states[addr]; ok {
		// We've already registered an error. Since we use connections concurrently
		// we want to avoid double backing-off (and double retrying) here.
		switch st.retry {
		case retryWait:
			// waiting for retry to be scheduled
			return
		case retryReady:
			// scheduled, waiting to be picked
			return
		case retryRunning:
			// backoff: we're here because we hit an error during the retry
			ws = st
			ws.wait *= 2
			ws.retry = retryWait
			if ws.wait > maxRetryWait {
				ws.wait = maxRetryWait
			}
		default:
			panic(fmt.Errorf("unexpected retry state: %s", st.retry))
		}
	}
	ss.setState(addr, ws)

	if ws.retry == retryWait {
		go ss.scheduleRetry(addr, ws.wait)
	}
}

// Sets the retry state for an address, and updates the available list.
// MUST be called from a method with a lock on the mutex, or will cause concurrent map crashes
func (ss *serversWithBreaker) setState(addr net.Addr, ws waitState) {
	if ss.states == nil {
		ss.states = map[net.Addr]waitState{}
	}
	ss.states[addr] = ws
	ss.filterAvailable()
}

// MUST be called from a method with a lock on the mutex, or will cause concurrent map crashes
func (ss *serversWithBreaker) deleteState(addr net.Addr) {
	delete(ss.states, addr)
	ss.filterAvailable()
}

func (ss *serversWithBreaker) filterAvailable() {
	// start with nothing available, and add available servers (with duplicates for balancing)
	ss.available = ss.available[:0]
	for _, addr := range ss.addrs {
		if ws, ok := ss.states[addr]; !ok || ws.retry == retryReady {
			ss.available = append(ss.available, addr)
		}
	}
}

// is a network error, rather than a protocol level error like ErrCacheMiss
func isConnectionError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF)
}

func (ss *serversWithBreaker) scheduleRetry(addr net.Addr, wait time.Duration) {
	time.Sleep(wait)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ws, ok := ss.states[addr]
	if !ok {
		// recovered while routine was sleeping
		return
	}
	if ws.retry != retryWait {
		panic(fmt.Errorf("retry unexpectedly not in RETRY_WAIT: %s", ws.retry))
	}
	// schedule a retry
	ws.retry = retryReady
	ss.setState(addr, ws)
}
