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

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"hash/crc32"
	"os"
	"net"
	"sync"
)

type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, os.Error)
}

type StaticServerSelector struct {
	lk    sync.RWMutex
	addrs []net.Addr
}

func (ss *StaticServerSelector) SetServers(servers ...string) os.Error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		tcpaddr, err := net.ResolveTCPAddr("tcp", server)
		if err != nil {
			return err
		}
		naddr[i] = tcpaddr
	}

	ss.lk.Lock()
	defer ss.lk.Unlock()
	ss.addrs = naddr
	return nil
}

var ErrNoServers = os.NewError("memcache: no servers configured or available")

func (ss *StaticServerSelector) PickServer(key string) (net.Addr, os.Error) {
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	// TODO-GO: remove this copy
	cs := crc32.ChecksumIEEE([]byte(key))
	return ss.addrs[cs%uint32(len(ss.addrs))], nil
}
