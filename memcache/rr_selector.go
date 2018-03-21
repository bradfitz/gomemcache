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
	errCount       = 3               // Number of times we allow a server to run into error before we start skipping it
	skipTimeBase   = time.Second * 5 // Time delay until we retry the server again
	extraWaitRange = 3               // Randomize the delay a little bit between clients to prevent the thundering herd problem
)

type rrServer struct {
	addr           net.Addr
	missCount      int // Consecutive number of misses on this particular server
	errCount       int
	skipDeadline   time.Time
	lastQueueIndex int
	lastQueue      *rrQueue
	queues         []rrQueue
}

type rrQueue struct {
	keyIndex     int
	missCount    int // Consecutive number of misses on this particular queue
	skipDeadline time.Time
}

type RRServerList struct {
	mu         sync.RWMutex
	servers    []rrServer
	lastIndex  int
	lastServer *rrServer
	keys       []string
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
	rrsl.lastIndex = 0
	rrsl.lastServer = nil

	if len(rrsl.keys) > 0 {
		rrsl.SetQueuesUnlocked(rrsl.keys)
	}
	return nil
}

func (rrsl *RRServerList) SetQueues(queues ...string) {
	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()

	rrsl.SetQueuesUnlocked(queues)
}

func (rrsl *RRServerList) SetQueuesUnlocked(queues []string) {
	rrsl.keys = queues
	for i := 0; i < len(rrsl.servers); i++ {
		rrsl.servers[i].queues = make([]rrQueue, 0, len(rrsl.keys))
		for j := 0; j < len(rrsl.keys); j++ {
			queue := rrQueue{
				keyIndex:  j,
				missCount: 0,
			}
			rrsl.servers[i].queues = append(rrsl.servers[i].queues, queue)
			rrsl.servers[i].lastQueueIndex = 0
			rrsl.servers[i].lastQueue = nil
		}
	}
}

func (rrsl *RRServerList) GetQueues() []string {
	return rrsl.keys
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

func (rrsl *RRServerList) PickQueue() (net.Addr, string, error) {
	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()

	// All servers should have exactly the same queue list
	// so we only need to check the first server here
	if len(rrsl.servers) == 0 || len(rrsl.servers[0].queues) == 0 {
		return nil, "", ErrNoServers
	}

	// Start from the last server because we don't want to starve
	// out the other queues every time there's a hit on this server
	index := rrsl.lastIndex
	for {
		server := &rrsl.servers[index]
		if server.errCount >= errCount && time.Now().Before(server.skipDeadline) {
			// The current server is misbehaving, skip it and check the other servers
			index++
			if index >= len(rrsl.servers) {
				index = 0
			}
			if index == rrsl.lastIndex {
				break
			}
			continue
		}

		// Next pick a queue for this server
		queueFound := false
		queueIndex := server.lastQueueIndex + 1
		for !queueFound {
			if queueIndex >= len(server.queues) {
				queueIndex = 0
			}
			queue := &server.queues[queueIndex]
			if queue.missCount >= missCount {
				if time.Now().After(queue.skipDeadline) {
					queueFound = true
					rrsl.lastIndex = index
					server.lastQueueIndex = queueIndex
				}
			} else {
				queueFound = true
				rrsl.lastIndex = index
				server.lastQueueIndex = queueIndex
			}
			queueIndex++
			if queueIndex == server.lastQueueIndex+1 {
				break
			}
		}
		if !queueFound {
			// This server doesn't have an available queue
			// Check the other servers
			index++
			if index >= len(rrsl.servers) {
				index = 0
			}
			if index == rrsl.lastIndex {
				break
			}
			continue
		}
		rrsl.lastServer = &rrsl.servers[rrsl.lastIndex]
		server.lastQueue = &server.queues[server.lastQueueIndex]

		return server.addr, rrsl.keys[server.lastQueue.keyIndex], nil
	}

	return nil, "", ErrNoServers
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
	for !found {
		if index >= len(rrsl.servers) {
			index = 0
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
		if index == rrsl.lastIndex+1 {
			break
		}
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

func (rrsl *RRServerList) QueueCacheMiss() {
	if rrsl.lastServer == nil || rrsl.lastServer.lastQueue == nil {
		return
	}

	rrsl.lastServer.errCount = 0
	rrsl.lastServer.lastQueue.missCount++
	if rrsl.lastServer.lastQueue.missCount >= missCount {
		rrsl.lastServer.lastQueue.missCount = missCount
		// When we set the deadline randomize it a bit per client so that all
		// thread's don't wake up around the same time and hammer the servers
		extraWait := time.Duration(rand.Int31n(extraWaitRange))
		rrsl.lastServer.lastQueue.skipDeadline = time.Now().Add(skipTimeBase + time.Second*extraWait)
	}
}

func (rrsl *RRServerList) QueueCacheHit() {
	rrsl.lastServer.lastQueue.missCount = 0
	rrsl.lastServer.errCount = 0
}

func (rrsl *RRServerList) ServerMisbehaving() {
	if rrsl.lastServer == nil {
		return
	}

	rrsl.lastServer.errCount++
	if rrsl.lastServer.errCount >= errCount {
		rrsl.lastServer.errCount = errCount
		extraWait := time.Duration(rand.Int31n(extraWaitRange))
		rrsl.lastServer.skipDeadline = time.Now().Add(skipTimeBase + time.Second*extraWait)
	}
}

func (rrsl *RRServerList) GetCurrQueue() (net.Addr, string) {
	currQueue := rrsl.lastServer.lastQueue
	return rrsl.lastServer.addr, rrsl.keys[currQueue.keyIndex]
}
