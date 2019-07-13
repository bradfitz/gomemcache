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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var cachedumpItem = regexp.MustCompile("ITEM (.*) \\[(\\d+) b; (\\d+) s\\]")

//DumpKey information of a stored key.
type DumpKey struct {
	// Key The item key
	Key string
	// Size Item size (including key) in bytes
	Size uint64
	// Expiration expiration time, as a unix timestamp.
	Expiration uint64
}

// StatsItemsServer information about item storage per slab class.
type StatsItemsServer struct {
	// ServerErr Error if can't get the stats information.
	ServerErr error
	// StatsItemsSlabs statistics for each slab.
	StatsItemsSlabs map[string]*StatsItems
	// DumpErr Error if can't cachedump
	DumpErr error
}

// StatsItems information of the items in the slab.
type StatsItems struct {
	// Errs contains the errors that occurred while parsing the response
	Errs errorsSlice
	//Retrieved dumped keys
	Keys []DumpKey

	// Number Number of items presently stored in this class. Expired items are not automatically excluded.
	Number uint64
	// NumberHot Number of items presently stored in the HOT LRU.
	NumberHot uint64
	// NumberWarm Number of items presently stored in the WARM LRU.
	NumberWarm uint64
	// NumberCold Number of items presently stored in the COLD LRU.
	NumberCold uint64
	// NumberTemp Number of items presently stored in the TEMPORARY LRU.
	NumberTemp uint64
	// AgeHot Age of the oldest item in HOT LRU.
	AgeHot uint64
	// AgeWarm Age of the oldest item in WARM LRU.
	AgeWarm uint64
	// Age Age of the oldest item in the LRU.
	Age uint64
	// Evicted Number of times an item had to be evicted from the LRU  before it expired.
	Evicted uint64
	// EvictedNonzero Number of times an item which had an explicit expire time set had to be evicted from the LRU before it expired.
	EvictedNonzero uint64
	// EvictedTime Seconds since the last access for the most recent item evicted from this class. Use this to judge how recently active your evicted data is.
	EvictedTime uint64
	// Outofmemory Number of times the underlying slab class was unable to store a new item. This means you are running with -M or an eviction failed.
	Outofmemory uint64
	// Tailrepairs Number of times we self-healed a slab with a refcount leak. If this counter is increasing a lot, please report your situation to the developers.
	Tailrepairs uint64
	// Reclaimed Number of times an entry was stored using memory from an expired entry.
	Reclaimed uint64
	// ExpiredUnfetched Number of expired items reclaimed from the LRU which were never touched after being set.
	ExpiredUnfetched uint64
	// EvictedUnfetched Number of valid items evicted from the LRU which were never touched after being set.
	EvictedUnfetched uint64
	// EvictedActive Number of valid items evicted from the LRU which were recently touched but were evicted before being moved to the top of the LRU again.
	EvictedActive uint64
	// CrawlerReclaimed Number of items freed by the LRU Crawler.
	CrawlerReclaimed uint64
	// LrutailReflocked Number of items found to be refcount locked in the LRU tail.
	LrutailReflocked uint64
	// MovesToCold Number of items moved from HOT or WARM into COLD.
	MovesToCold uint64
	// MovesToWarm Number of items moved from COLD to WARM.
	MovesToWarm uint64
	// MovesWithinLru Number of times active items were bumped within HOT or WARM.
	MovesWithinLru uint64
	// DirectReclaims Number of times worker threads had to directly pull LRU tails to find memory for a new item.
	DirectReclaims uint64
	// HitsToHot Number of keys that have been requested and found present in the HOT LRU.
	HitsToHot uint64
	// HitsToWarm Number of keys that have been requested and found present in the WARM LRU.
	HitsToWarm uint64
	// HitsToCold Number of keys that have been requested and found present in the COLD LRU.
	HitsToCold uint64
	// HitsToTemp Number of keys that have been requested and found present in each sub-LRU.
	HitsToTemp uint64
}

// StatsItemsServers returns information about item storage per slab class of all the servers,
// retrieved with the `stats items` command
//
// maxDumpKeys is the maximum number of keys that are going to be retrieved
// if maxDumpKeys < 0, doesn't dump the keys
func (c *Client) StatsItemsServers(maxDumpKeys int) (servers map[net.Addr]*StatsItemsServer, err error) {
	// Each ServerStats has its own "asociated" sync.Mutex.
	servers = make(map[net.Addr]*StatsItemsServer)
	muxes := make(map[net.Addr]*sync.Mutex)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	// For each server addres a StatsItemsServer, sync.mutex is created.
	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = &StatsItemsServer{StatsItemsSlabs: make(map[string]*StatsItems)}
			muxes[addr] = new(sync.Mutex)
			return nil
		},
	)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(addrs))
	for _, addr := range addrs {
		go func(addr net.Addr) {
			mux := muxes[addr]
			server := servers[addr]
			c.statsItemsFromAddr(server, mux, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()

	wg.Add(len(addrs))
	if maxDumpKeys >= 0 {
		for _, addr := range addrs {
			go func(addr net.Addr) {
				mux := muxes[addr]
				server := servers[addr]
				c.cachedumpFromAddr(server, maxDumpKeys, mux, addr)
				wg.Done()
			}(addr)
		}
	}
	wg.Wait()
	return
}

// From the protocol definition for the command stats items.
//
// ------------------------------------------------------------------------
// Item statistics.
//
// CAVEAT: This section describes statistics which are subject to change in the
// future.
//
// The "stats" command with the argument of "items" returns information about
// item storage per slab class. The data is returned in the format:
//
// STAT items:<slabclass>:<stat> <value>\r\n
//
// The server terminates this list with the line.
//
// END\r\n
//
// The slabclass aligns with class ids used by the "stats slabs" command. Where
// "stats slabs" describes size and memory usage, "stats items" shows higher
// level information.
//
// The following item values are defined as of writing.
// ------------------------------------------------------------------------
func (c *Client) statsItemsFromAddr(sis *StatsItemsServer, mux *sync.Mutex, addr net.Addr) {
	var wg sync.WaitGroup
	err := c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats items\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		for {
			line, err := rw.ReadBytes('\n')
			if err != nil {
				return err
			}
			if bytes.Equal(line, resultEnd) {
				return nil
			}

			wg.Add(1)
			go func() {
				// STAT items:<slabclass>:<stat> <value>\r\n
				sis.parseStat(string(line[:len(line)-2]), mux)
				wg.Done()
			}()
		}
	})
	wg.Wait()
	sis.ServerErr = err
	return
}

func (sis *StatsItemsServer) parseStat(line string, mux *sync.Mutex) {
	// STAT items:<slabclass>:<stat> <value>
	tkns := strings.Split(line, ":")
	statValue := strings.Split(tkns[2], " ")
	mux.Lock()
	defer mux.Unlock()

	slab := sis.StatsItemsSlabs[tkns[1]]
	if slab == nil {
		slab = new(StatsItems)
		sis.StatsItemsSlabs[tkns[1]] = slab
	}
	slab.setStatValue(statValue[0], statValue[1])
}

func (si *StatsItems) setStatValue(stat, value string) {
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		err = errors.New("Unable to parse value for " + stat + " stat " + err.Error())
		si.Errs.AppendError(err)
		return
	}
	switch stat {
	case "number":
		si.Number = val
	case "number_hot":
		si.NumberHot = val
	case "number_warm":
		si.NumberWarm = val
	case "number_cold":
		si.NumberCold = val
	case "number_temp":
		si.NumberTemp = val
	case "age_hot":
		si.AgeHot = val
	case "age_warm":
		si.AgeWarm = val
	case "age":
		si.Age = val
	case "evicted":
		si.Evicted = val
	case "evicted_nonzero":
		si.EvictedNonzero = val
	case "evicted_time":
		si.EvictedTime = val
	case "outofmemory":
		si.Outofmemory = val
	case "tailrepairs":
		si.Tailrepairs = val
	case "reclaimed":
		si.Reclaimed = val
	case "expired_unfetched":
		si.ExpiredUnfetched = val
	case "evicted_unfetched":
		si.EvictedUnfetched = val
	case "evicted_active":
		si.EvictedActive = val
	case "crawler_reclaimed":
		si.CrawlerReclaimed = val
	case "lrutail_reflocked":
		si.LrutailReflocked = val
	case "moves_to_cold":
		si.MovesToCold = val
	case "moves_to_warm":
		si.MovesToWarm = val
	case "moves_within_lru":
		si.MovesWithinLru = val
	case "direct_reclaims":
		si.DirectReclaims = val
	case "hits_to_hot":
		si.HitsToHot = val
	case "hits_to_warm":
		si.HitsToWarm = val
	case "hits_to_cold":
		si.HitsToCold = val
	case "hits_to_temp":
		si.HitsToTemp = val
	}
}

func (c *Client) cachedumpFromAddr(sis *StatsItemsServer, maxDumpKeys int, mux *sync.Mutex, addr net.Addr) {
	var wg sync.WaitGroup
	l := len(sis.StatsItemsSlabs)
	if l == 0 {
		return
	}
	err := c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "stats cachedump\r\n")
		if err != nil {
			return err
		}
		if bytes.Equal(line, []byte("CLIENT_ERROR stats cachedump not allowed\r\n")) {
			return ErrDumpDisable
		}
		return nil
	})
	if err != nil {
		sis.DumpErr = err
	}

	wg.Add(l)
	for slab, stats := range sis.StatsItemsSlabs {
		go func(slab string, stats *StatsItems) {
			c.cachedumpSlabFromAddr(slab, stats, maxDumpKeys, mux, addr)
			wg.Done()
		}(slab, stats)
	}
	wg.Wait()
	//
	return
}

func (c *Client) cachedumpSlabFromAddr(slab string, si *StatsItems, maxDumpKeys int, mux *sync.Mutex, addr net.Addr) (err error) {
	var wg sync.WaitGroup
	err = c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats cachedump %s %d\r\n", slab, maxDumpKeys); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		for {
			line, err := rw.ReadBytes('\n')
			if err != nil {
				return err
			}
			if bytes.Equal(line, resultEnd) {
				return nil
			}
			wg.Add(1)
			go func() {
				//ITEM <key> [<bytes> b; <expiration> s]\r\n
				si.parseAddDumpKey(string(line), mux)
				wg.Done()
			}()
		}
	})
	wg.Wait()
	return
}

func (si *StatsItems) parseAddDumpKey(line string, mux *sync.Mutex) {
	tkns := cachedumpItem.FindStringSubmatch(line)
	if len(tkns) != 4 {
		mux.Lock()
		si.Errs.AppendError(fmt.Errorf("regex didn't match response correctly"))
		mux.Unlock()
		return
	}
	dk := DumpKey{Key: tkns[1]}
	dk.Size, _ = strconv.ParseUint(tkns[2], 10, 64)
	dk.Expiration, _ = strconv.ParseUint(tkns[3], 10, 64)
	mux.Lock()
	si.Keys = append(si.Keys, dk)
	mux.Unlock()
}
