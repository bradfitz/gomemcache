package memcache

import (
	"fmt"
	"hash"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/spaolacci/murmur3" // Import MurmurHash3
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

// JumpConsistentHash is a ServerSelector implementation using jump consistent hashing.
type JumpConsistentHash struct {
	mu           sync.RWMutex
	replicaCount int
	hashMap      map[string]int // Key: hashed key string, Value: server index
	servers      []string
	hashFunc     hash.Hash // MurmurHash3 function
}

// NewJumpConsistentHash creates a new JumpConsistentHash instance.
func NewJumpConsistentHash(replicaCount int, servers ...string) *JumpConsistentHash {
	if replicaCount <= 0 {
		return nil
	}
	h := &JumpConsistentHash{
		replicaCount: replicaCount,
		hashMap:      make(map[string]int),
		servers:      servers,
		hashFunc:     murmur3.New32(), // Create MurmurHash3 function
	}
	err := h.addServers()
	if err != nil {
		return h
	}
	return nil
}

// addServers adds all servers to the jump consistent hash ring with replicas.
func (h *JumpConsistentHash) addServers() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, server := range h.servers {
		for j := 0; j < h.replicaCount; j++ {
			key := fmt.Sprintf("%s:%d", server, j)
			h.hashFunc.Write([]byte(key))
			h.hashMap[fmt.Sprintf("%x", h.hashFunc.Sum(nil))] = i // Store server index
			h.hashFunc.Reset()                                    // Reset hash for next replica
		}
	}
	return nil
}

// PickServer finds the server responsible for a given key using jump consistent hashing.
func (h *JumpConsistentHash) PickServer(key string) (net.Addr, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.servers) == 0 {
		return nil, ErrNoServers
	}

	h.hashFunc.Write([]byte(key)) // Reuse existing MurmurHash3 function
	hashedKey := fmt.Sprintf("%x", h.hashFunc.Sum(nil))

	var serverIndex int
	for { // Loop until a server is found
		jump := (func() int {
			h.mu.RUnlock()     // Release read lock while generating random number
			defer h.mu.RLock() // Reacquire read lock before accessing hashMap
			return rand.Intn(len(h.hashMap))
		})()
		jumpKey := fmt.Sprintf("%x", murmur3.New32().Sum([]byte(fmt.Sprintf("%x:%d", hashedKey, jump)))) // Create new MurmurHash3 for jump
		for k, v := range h.hashMap {
			if k >= jumpKey {
				serverIndex = v
				break
			}
		}
		if serverIndex != 0 {
			break // Found a server responsible for the jumpKey
		}
	}

	if serverIndex == 0 {
		serverIndex = len(h.hashMap) // Wrap around to first server if no match found
	}

	return h.getServerAddr(serverIndex)
}

// getServerAddr returns the net.Addr for a given server index.
func (h *JumpConsistentHash) getServerAddr(index int) (net.Addr, error) {
	if index < 0 || index >= len(h.servers) {
		return nil, fmt.Errorf("invalid server index: %d", index)
	}
	server := h.servers[index]
	if strings.Contains(server, "/") {
		return net.ResolveUnixAddr("unix", server)
	}
	return net.ResolveTCPAddr("tcp", server)
}

// Each iterates over each server calling the given function
func (h *JumpConsistentHash) Each(f func(net.Addr) error) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Sort servers for deterministic iteration
	sortedServers := make([]string, len(h.servers))
	copy(sortedServers, h.servers)
	sort.Strings(sortedServers)

	for _, server := range sortedServers {
		addr, err := h.getServerAddr(h.hashMap[fmt.Sprintf("%x", murmur3.New32().Sum([]byte(server)))]) // Find server index using key hash
		if err != nil {
			return err
		}
		if err := f(addr); err != nil {
			return err
		}
	}
	return nil
}
