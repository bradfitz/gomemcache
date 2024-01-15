package memcache

import (
	"net"
	"strings"
	"sync"
)

// NewRoundRobin returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func NewRoundRobin(server ...string) *Client {
	ss := new(RoundRobinServerList)
	err := ss.SetServers(server...)
	if err != nil {
		return nil
	}

	return NewFromRoundRobinSelector(ss)
}

// NewFromRoundRobinSelector returns a new Client using the provided RoundRobinServerSelector.
func NewFromRoundRobinSelector(ss *RoundRobinServerList) *Client {
	return &Client{
		selector: ss,
		DisableCAS: false,
	}
}

// RoundRobinServerList is a simple ServerSelector. Its zero value is usable.
type RoundRobinServerList struct {
	mu    sync.Mutex
	addrs []net.Addr
	next int
}

// SetServers changes a RoundRobinServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the RoundRobinServerList.
func (ss *RoundRobinServerList) SetServers(servers ...string) error {
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
	return nil
}

// Each iterates over each server calling the given function
func (ss *RoundRobinServerList) Each(f func(net.Addr) error) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

func (ss *RoundRobinServerList) PickServer(key string) (net.Addr, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}

	ss.next = (ss.next + 1) % len(ss.addrs)

	return ss.addrs[ss.next], nil
}
