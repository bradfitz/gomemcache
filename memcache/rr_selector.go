package memcache

import (
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	missCount      = 3               // Number of times we allow a server to get a cache miss before we start skipping it
	skipTimeBase   = time.Second * 5 // Time delay until we retry the server again
	extraWaitRange = 3               // Randomize the delay a little bit between clients to prevent the thundering herd problem
)

type rrServer struct {
	addr         net.Addr
	missCount    int // Consecutive number of misses on this particular server
	skipDeadline time.Time
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
		server := &rrsl.servers[index]
		if server.missCount >= missCount {
			if time.Now().After(server.skipDeadline) {
				found = true
				rrsl.lastIndex = index
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
	if rrsl.lastServer.missCount >= missCount {
		rrsl.lastServer.missCount = missCount
		// When we set the deadline randomize it a bit per client so that all
		// thread's don't wake up around the same time and hammer the servers
		extraWait := time.Duration(rand.Int31n(extraWaitRange))
		rrsl.lastServer.skipDeadline = time.Now().Add(skipTimeBase + time.Second*extraWait)
	}
}

// Record a cache hit on the server to put it back in the regular rotation
func (rrsl *RRServerList) CacheHit() {
	rrsl.lastServer.missCount = 0
}
