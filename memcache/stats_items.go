/*
Copyright 2019 gomemcache Authors

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
	"reflect"
	"regexp"
	"strconv"
	"sync"
)

var cachedumpItem = regexp.MustCompile("ITEM (.*) \\[(\\d+) b; (\\d+) s\\]")

// DumpKey information of a stored key.
type DumpKey struct {
	// Key item key.
	Key string
	// Size item size (including key) in bytes.
	Size uint64
	// Expiration expiration time, as a unix timestamp.
	Expiration uint64
}

// StatsItemsServer information about item storage per slab class.
type StatsItemsServer struct {
	// ServerErr irror if can't get the stats information.
	ServerErr error
	// StatsItemsSlabs statistics for each slab.
	StatsItemsSlabs map[string]*StatsItems
	// DumpErr error if can't cachedump.
	DumpErr error
}

// StatsItems information of the items in the slab.
type StatsItems struct {
	// Errs contains the errors that occurred while parsing the response.
	Errs errorsSlice
	// Keys retrieved dumped keys.
	Keys []DumpKey
	// UnknownStats contains the stats that are not in the struct. This can be useful if the memcached developers add new stats in newer versions.
	UnknownStats map[string]string

	// Number number of items presently stored in this class. Expired items are not automatically excluded.
	Number uint64
	// NumberHot number of items presently stored in the HOT LRU.
	NumberHot uint64
	// NumberWarm number of items presently stored in the WARM LRU.
	NumberWarm uint64
	// NumberCold number of items presently stored in the COLD LRU.
	NumberCold uint64
	// NumberTemp number of items presently stored in the TEMPORARY LRU.
	NumberTemp uint64
	// AgeHot age of the oldest item in HOT LRU.
	AgeHot uint64
	// AgeWarm age of the oldest item in WARM LRU.
	AgeWarm uint64
	// Age age of the oldest item in the LRU.
	Age uint64
	// Evicted number of times an item had to be evicted from the LRU  before it expired.
	Evicted uint64
	// EvictedNonzero number of times an item which had an explicit expire time set had to be evicted from the LRU before it expired.
	EvictedNonzero uint64
	// EvictedTime seconds since the last access for the most recent item evicted from this class. Use this to judge how recently active your evicted data is.
	EvictedTime uint64
	// Outofmemory number of times the underlying slab class was unable to store a new item. This means you are running with -M or an eviction failed.
	Outofmemory uint64
	// Tailrepairs number of times we self-healed a slab with a refcount leak. If this counter is increasing a lot, please report your situation to the developers.
	Tailrepairs uint64
	// Reclaimed number of times an entry was stored using memory from an expired entry.
	Reclaimed uint64
	// ExpiredUnfetched number of expired items reclaimed from the LRU which were never touched after being set.
	ExpiredUnfetched uint64
	// EvictedUnfetched number of valid items evicted from the LRU which were never touched after being set.
	EvictedUnfetched uint64
	// EvictedActive number of valid items evicted from the LRU which were recently touched but were evicted before being moved to the top of the LRU again.
	EvictedActive uint64
	// CrawlerReclaimed number of items freed by the LRU Crawler.
	CrawlerReclaimed uint64
	// LrutailReflocked number of items found to be refcount locked in the LRU tail.
	LrutailReflocked uint64
	// MovesToCold number of items moved from HOT or WARM into COLD.
	MovesToCold uint64
	// MovesToWarm number of items moved from COLD to WARM.
	MovesToWarm uint64
	// MovesWithinLru number of times active items were bumped within HOT or WARM.
	MovesWithinLru uint64
	// DirectReclaims number of times worker threads had to directly pull LRU tails to find memory for a new item.
	DirectReclaims uint64
	// HitsToHot number of keys that have been requested and found present in the HOT LRU.
	HitsToHot uint64
	// HitsToWarm number of keys that have been requested and found present in the WARM LRU.
	HitsToWarm uint64
	// HitsToCold number of keys that have been requested and found present in the COLD LRU.
	HitsToCold uint64
	// HitsToTemp number of keys that have been requested and found present in each sub-LRU.
	HitsToTemp uint64
}

// StatsItemsServers returns information about item storage per slab class of all the servers,
// retrieved with the `stats items` command.
//
// maxDumpKeys is the maximum number of keys that are going to be retrieved
// if maxDumpKeys < 0, doesn't dump the keys.
func (c *Client) StatsItemsServers(maxDumpKeys int) (servers map[net.Addr]*StatsItemsServer, err error) {
	servers = make(map[net.Addr]*StatsItemsServer)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = &StatsItemsServer{StatsItemsSlabs: make(map[string]*StatsItems)}
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
			server := servers[addr]
			c.statsItemsFromAddr(server, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()

	if maxDumpKeys >= 0 {
		wg.Add(len(addrs))
		for _, addr := range addrs {
			go func(addr net.Addr) {
				server := servers[addr]
				c.cachedumpFromAddr(server, maxDumpKeys, addr)
				wg.Done()
			}(addr)
		}
		wg.Wait()
	}
	return
}

// From the protocol definition for the command stats items:
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
// The server terminates this list with the line:
//
// END\r\n
//
// The slabclass aligns with class ids used by the "stats slabs" command. Where
// "stats slabs" describes size and memory usage, "stats items" shows higher
// level information.
//
// The following item values are defined as of writing.
// ------------------------------------------------------------------------
func (c *Client) statsItemsFromAddr(sis *StatsItemsServer, addr net.Addr) {
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

			// STAT items:<slabclass>:<stat> <value>
			sis.parseStat(line[11 : len(line)-2])
		}
	})
	sis.ServerErr = err
	return
}

func (sis *StatsItemsServer) parseStat(line []byte) {
	// <slabclass>:<stat> <value>
	tkns := bytes.FieldsFunc(line, func(r rune) bool {
		return r == ':' || r == ' '
	})
	slabclass := string(tkns[0])
	slab := sis.StatsItemsSlabs[slabclass]
	if slab == nil {
		slab = new(StatsItems)
		slab.UnknownStats = make(map[string]string)
		sis.StatsItemsSlabs[slabclass] = slab
	}
	err := parseSetStatValue(reflect.ValueOf(slab), string(tkns[1]), string(tkns[2]))
	if err != nil {
		if err != errUnknownStat {
			slab.Errs.AppendError(err)
		} else {
			slab.UnknownStats[string(tkns[1])] = string(tkns[2])
		}
	}
}

func (c *Client) cachedumpFromAddr(sis *StatsItemsServer, maxDumpKeys int, addr net.Addr) {
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
		return
	}

	wg.Add(l)
	for slab, stats := range sis.StatsItemsSlabs {
		go func(slab string, stats *StatsItems) {
			c.cachedumpSlabFromAddr(slab, stats, maxDumpKeys, addr)
			wg.Done()
		}(slab, stats)
	}
	wg.Wait()
	return
}

func (c *Client) cachedumpSlabFromAddr(slab string, si *StatsItems, maxDumpKeys int, addr net.Addr) (err error) {
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
			// ITEM <key> [<bytes> b; <expiration> s]\r\n
			si.parseAddDumpKey(string(line))
		}
	})
	return
}

func (si *StatsItems) parseAddDumpKey(line string) {
	tkns := cachedumpItem.FindStringSubmatch(line)
	if len(tkns) != 4 {
		si.Errs.AppendError(errors.New("regex didn't match response correctly"))
		return
	}
	dk := DumpKey{Key: tkns[1]}
	dk.Size, _ = strconv.ParseUint(tkns[2], 10, 64)
	dk.Expiration, _ = strconv.ParseUint(tkns[3], 10, 64)
	si.Keys = append(si.Keys, dk)
}
