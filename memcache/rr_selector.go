package memcache

import (
	"net"
	"strings"
	"sync"
)

type RRServerList struct {
	mu		   sync.RWMutex
	addrs	   []net.Addr
	lastIndex  int
	lastServer net.Addr
}

func (rrsl *RRServerList) SetServers(servers ...string) error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = addr
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = tcpaddr
		}
	}

	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()
	rrsl.addrs = naddr
	return nil
}

func (rrsl *RRServerList) Each(f func(net.Addr) error) error {
	rrsl.mu.RLock()
	defer rrsl.mu.RUnlock()

	for _, addr := range rrsl.addrs {
		err := f(addr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rrsl *RRServerList) PickServer(key string) (net.Addr, error) {
	rrsl.mu.Lock()
	defer rrsl.mu.Unlock()

	if len(rrsl.addrs) == 0 {
		return nil, ErrNoServers
	}
	if len(rrsl.addrs) == 1 {
		return rrsl.addrs[0], nil
	}
	if strings.Contains(key, "/close") && !strings.Contains(key, "/close/open") {
		return rrsl.lastServer, nil
	}
	if rrsl.lastIndex >= len(rrsl.addrs) {
		rrsl.lastIndex = 0
	}

	rrsl.lastServer = rrsl.addrs[rrsl.lastIndex]
	addr := rrsl.lastServer
	rrsl.lastIndex++

	return addr, nil
}
