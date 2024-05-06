package memcache

import (
	"net"
	"strings"
	"sync"

	"github.com/dgryski/go-rendezvous"
	"github.com/spaolacci/murmur3"
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
	Each(func(net.Addr) error) error
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	mu               sync.RWMutex
	addrs            []net.Addr
	rendezvousHasher *rendezvous.Rendezvous
	strAddrs         map[string]net.Addr
}

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

func (s *ServerList) NewRendezvousHash(servers ...string) *rendezvous.Rendezvous {
	var nodes []string
	nodes = append(nodes, servers...)
	hrw := rendezvous.New(nodes, func(s string) uint64 {
		return murmur3.Sum64([]byte(s)) // Using MurmurHash3 for hashing
	})
	return hrw
}

// SetServers changes a ServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error {
	naddr := make([]net.Addr, len(servers))
	strAddrs := make(map[string]net.Addr)
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			strAddrs[server] = newStaticAddr(addr)
			naddr[i] = newStaticAddr(addr)
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(tcpaddr)
			strAddrs[server] = newStaticAddr(tcpaddr)
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr
	ss.strAddrs = strAddrs
	hrw := ss.NewRendezvousHash(servers...)
	ss.rendezvousHasher = hrw
	return nil
}

// Each iterates over each server calling the given function
func (ss *ServerList) Each(f func(net.Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// PickServer finds the server responsible for a given key using jump consistent hashing.
func (ss *ServerList) PickServer(key string) (net.Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	node := ss.rendezvousHasher.Lookup(key)
	return ss.strAddrs[node], nil
}
