package memcache

import (
	"net"
	"strings"
	"sync"
)

const (
	missCount = 3   // Number of times we allow a server to get a cache miss before we start skipping it
	skipCount = 100 // Number of times to skip over this server in the selector before we try again
)

type rrServer struct {
	addr      net.Addr
	missCount int // Consecutive number of misses on this particular server
	skipCount int // Number of times we've skipped this server in the selector
}

type RRServerList struct {
	mu         sync.RWMutex
	servers    []rrServer
	lastIndex  int
	lastServer *rrServer
}

func (rrsl *RRServerList) SetServers(servers ...string) error {
	rrServers := make([]rrServer, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			rrServers[i].addr = addr
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			rrServers[i].addr = tcpaddr
		}
	}

	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()
	rrsl.servers = rrServers
	return nil
}

func (rrsl *RRServerList) Each(f func(net.Addr) error) error {
	rrsl.mu.RLock()
	defer rrsl.mu.RUnlock()

	for _, server := range rrsl.servers {
		err := f(server.addr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rrsl *RRServerList) PickServer(key string) (net.Addr, error) {
	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()

	if len(rrsl.servers) == 0 {
		return nil, ErrNoServers
	}
	if strings.Contains(key, "/close") && !strings.Contains(key, "/close/open") {
		return rrsl.lastServer.addr, nil
	}

	found := false
	index := rrsl.lastIndex + 1
	for index != rrsl.lastIndex && !found {
		if index >= len(rrsl.servers) {
			index = 0
			continue
		}
		server := rrsl.servers[index]
		if server.missCount >= missCount {
			if server.skipCount >= skipCount {
				server.skipCount = 0
				found = true
				rrsl.lastIndex = index
			} else {
				server.skipCount++
			}
		} else {
			found = true
			rrsl.lastIndex = index
		}
		index++
	}

	if !found {
		return nil, ErrNoServers
	}

	rrsl.lastServer = &rrsl.servers[rrsl.lastIndex]
	addr := rrsl.lastServer.addr

	return addr, nil
}

// We use both of these functions unlocked anyway since we have to maintain some
// state for kestrel /open and /close calls for reading. This doesn't affect
// writing to kestrel

// Record a cache miss for the last server we hit so that we avoid hitting it
// too often
func (rrsl *RRServerList) CacheMiss() {
	rrsl.lastServer.missCount++
}

// Record a cache hit on the server to put it back in the regular rotation
func (rrsl *RRServerList) CacheHit() {
	rrsl.lastServer.missCount = 0
}
