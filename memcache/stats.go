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
	"strconv"
	"strings"
	"sync"
	"unicode"
)

type errorsSlice []error

var errUnknownStat = errors.New("unknown stat")

func (errs errorsSlice) Error() string {
	switch len(errs) {
	case 0:
		return ""
	case 1:
		return errs[0].Error()
	}
	n := (len(errs) - 1)
	for i := 0; i < len(errs); i++ {
		n += len(errs[i].Error())
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(errs[0].Error())
	for _, err := range errs[1:] {
		b.WriteByte(':')
		b.WriteString(err.Error())
	}
	return b.String()
}

func (errs *errorsSlice) AppendError(err error) {
	if err == nil {
		return
	}
	if errs == nil {
		errs = &errorsSlice{err}
		return
	}
	set := *errs
	set = append(set, err)
	*errs = set
}

// ServerStats contains the general statistics from one server.
type ServerStats struct {
	// ServerErr error if can't get the stats information.
	ServerErr error
	// Errs contains the errors that occurred while parsing the response.
	Errs errorsSlice
	// UnknownStats contains the stats that are not in the struct. This can be useful if the memcached developers add new stats in newer versions.
	UnknownStats map[string]string

	// Version version string of this server.
	Version string
	// AcceptingConns whether or not server is accepting conns.
	AcceptingConns bool
	// HashIsExpanding indicates if the hash table is being grown to a new size.
	HashIsExpanding bool
	// SlabReassignRunning if a slab page is being moved.
	SlabReassignRunning bool
	// Pid process id of this server process.
	Pid uint32
	// Uptime number of secs since the server started.
	Uptime uint32
	// Time current UNIX time according to the server.
	Time uint32
	// RusageUser accumulated user time for this process (seconds:microseconds).
	RusageUser float64
	// RusageSystem accumulated system time for this process (seconds:microseconds).
	RusageSystem float64
	// MaxConnections max number of simultaneous connections.
	MaxConnections uint32
	// CurrConnections number of open connections.
	CurrConnections uint32
	// TotalConnections total number of connections opened since the server started running.
	TotalConnections uint32
	// ConnectionStructures number of connection structures allocated by the server.
	ConnectionStructures uint32
	// ReservedFds number of misc fds used internally.
	ReservedFds uint32
	// Threads number of worker threads requested. (see doc/threads.txt).
	Threads uint32
	// HashPowerLevel current size multiplier for hash table.
	HashPowerLevel uint32
	// SlabGlobalPagePool slab pages returned to global pool for reassignment to other slab classes.
	SlabGlobalPagePool uint32
	// PointerSize default size of pointers on the host OS (generally 32 or 64).
	PointerSize uint64
	// CurrItems current number of items stored.
	CurrItems uint64
	// TotalItems total number of items stored since the server started.
	TotalItems uint64
	// Bytes current number of bytes used to store items.
	Bytes uint64
	// RejectedConnections conns rejected in maxconns_fast mode.
	RejectedConnections uint64
	// CmdGet cumulative number of retrieval reqs.
	CmdGet uint64
	// CmdSet cumulative number of storage reqs.
	CmdSet uint64
	// CmdFlush cumulative number of flush reqs.
	CmdFlush uint64
	// CmdTouch cumulative number of touch reqs.
	CmdTouch uint64
	// GetHits number of keys that have been requested and found present.
	GetHits uint64
	// GetMisses number of items that have been requested and not found.
	GetMisses uint64
	// GetExpired number of items that have been requested but had already expired.
	GetExpired uint64
	// GetFlushed number of items that have been requested but have been flushed via flush_all.
	GetFlushed uint64
	// DeleteMisses number of deletions reqs for missing keys.
	DeleteMisses uint64
	// DeleteHits number of deletion reqs resulting in an item being removed.
	DeleteHits uint64
	// IncrMisses number of incr reqs against missing keys.
	IncrMisses uint64
	// IncrHits number of successful incr reqs.
	IncrHits uint64
	// DecrMisses number of decr reqs against missing keys.
	DecrMisses uint64
	// DecrHits number of successful decr reqs.
	DecrHits uint64
	// CasMisses number of CAS reqs against missing keys.
	CasMisses uint64
	// CasHits number of successful CAS reqs.
	CasHits uint64
	// CasBadval number of CAS reqs for which a key was found, but the CAS value did not match.
	CasBadval uint64
	// TouchHits number of keys that have been touched with a new expiration time.
	TouchHits uint64
	// TouchMisses number of items that have been touched and not found.
	TouchMisses uint64
	// AuthCmds number of authentication commands handled, success or failure.
	AuthCmds uint64
	// AuthErrors number of failed authentications.
	AuthErrors uint64
	// IdleKicks number of connections closed due to reaching their idle timeout.
	IdleKicks uint64
	// Evictions number of valid items removed from cache to free memory for new items.
	Evictions uint64
	// Reclaimed number of times an entry was stored using memory from an expired entry.
	Reclaimed uint64
	// BytesRead total number of bytes read by this server from network.
	BytesRead uint64
	// BytesWritten total number of bytes sent by this server to network.
	BytesWritten uint64
	// LimitMaxbytes number of bytes this server is allowed to use for storage.
	LimitMaxbytes uint64
	// ListenDisabledNum number of times server has stopped accepting new connections (maxconns).
	ListenDisabledNum uint64
	// TimeInListenDisabledUs number of microseconds in maxconns.
	TimeInListenDisabledUs uint64
	// ConnYields number of times any connection yielded to another due to hitting the -R limit.
	ConnYields uint64
	// HashBytes bytes currently used by hash tables.
	HashBytes uint64
	// ExpiredUnfetched items pulled from LRU that were never touched by get/incr/append/etc before expiring.
	ExpiredUnfetched uint64
	// EvictedUnfetched items evicted from LRU that were never touched by get/incr/append/etc.
	EvictedUnfetched uint64
	// EvictedActive items evicted from LRU that had been hit recently but did not jump to top of LRU.
	EvictedActive uint64
	// SlabsMoved total slab pages moved.
	SlabsMoved uint64
	// CrawlerReclaimed total items freed by LRU Crawler.
	CrawlerReclaimed uint64
	// CrawlerItemsChecked total items examined by LRU Crawler.
	CrawlerItemsChecked uint64
	// LrutailReflocked times LRU tail was found with active ref. Items can be evicted to avoid OOM errors.
	LrutailReflocked uint64
	// MovesToCold items moved from HOT/WARM to COLD LRU's.
	MovesToCold uint64
	// MovesToWarm items moved from COLD to WARM LRU.
	MovesToWarm uint64
	// MovesWithinLru items reshuffled within HOT or WARM LRU's.
	MovesWithinLru uint64
	// DirectReclaims times worker threads had to directly reclaim or evict items.
	DirectReclaims uint64
	// LruCrawlerStarts times an LRU crawler was started.
	LruCrawlerStarts uint64
	// LruMaintainerJuggles number of times the LRU bg thread woke up.
	LruMaintainerJuggles uint64
	// SlabReassignRescues items rescued from eviction in page move.
	SlabReassignRescues uint64
	// SlabReassignEvictionsNomem valid items evicted during a page move (due to no free memory in slab).
	SlabReassignEvictionsNomem uint64
	// SlabReassignChunkRescues individual sections of an item rescued during a page move.
	SlabReassignChunkRescues uint64
	// SlabReassignInlineReclaim internal stat counter for when the page mover clears memory from the chunk freelist when it wasn't expecting to.
	SlabReassignInlineReclaim uint64
	// SlabReassignBusyItems items busy during page move, requiring a retry before page can be moved.
	SlabReassignBusyItems uint64
	// SlabReassignBusyDeletes items busy during page move, requiring deletion before page can be moved.
	SlabReassignBusyDeletes uint64
	// LogWorkerDropped logs a worker never wrote due to full buf.
	LogWorkerDropped uint64
	// LogWorkerWritten logs written by a worker, to be picked up.
	LogWorkerWritten uint64
	// LogWatcherSkipped logs not sent to slow watchers.
	LogWatcherSkipped uint64
	// LogWatcherSent logs written to watchers.
	LogWatcherSent uint64
	// Libevent libevent string version.
	Libevent string
	// LruCrawlerRunning crawl in progress.
	LruCrawlerRunning bool
	// MallocFails number of malloc fails.
	MallocFails uint64
	// LruBumpsDropped lru total bumps dropped.
	LruBumpsDropped uint64
}

// StatsServers returns the general statistics of the servers
// retrieved with the `stats` command.
func (c *Client) StatsServers() (servers map[net.Addr]*ServerStats, err error) {
	servers = make(map[net.Addr]*ServerStats)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = new(ServerStats)
			servers[addr].UnknownStats = make(map[string]string)
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
			c.statsFromAddr(server, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	return
}

func (c *Client) statsFromAddr(server *ServerStats, addr net.Addr) {
	err := c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats\r\n"); err != nil {
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
			// STAT <name> <value>\r\n
			tkns := bytes.Split(line[5:len(line)-2], space)
			err = parseSetStatValue(reflect.ValueOf(server), string(tkns[0]), string(tkns[1]))
			if err != nil {
				if err != errUnknownStat {
					server.Errs.AppendError(err)
				} else {
					server.UnknownStats[string(tkns[0])] = string(tkns[1])
				}
			}
		}
	})
	server.ServerErr = err
	return
}

// parseSetStatValue parses and sets the value of the stat to the corresponding struct field.
//
// In order to know the corresponding field of the stat, the snake_case stat name is converted to CamelCase
func parseSetStatValue(strPntr reflect.Value, stat, value string) error {
	statCamel := toCamel(stat)
	f := reflect.Indirect(strPntr).FieldByName(statCamel)
	switch f.Kind() {
	case reflect.Uint32:
		uintv, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return errors.New("unable to parse uint value " + stat + ":" + err.Error())
		}
		f.SetUint(uintv)
	case reflect.Uint64:
		uintv, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.New("unable to parse uint value " + stat + ":" + err.Error())
		}
		f.SetUint(uintv)
	case reflect.Float64:
		floatv, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.New("unable to parse float value for " + stat + ":" + err.Error())
		}
		f.SetFloat(floatv)
	case reflect.String:
		f.SetString(value)
	case reflect.Bool:
		f.SetBool(value == "1")
	default:
		return errUnknownStat
	}
	return nil
}

func toCamel(s string) string {
	if s == "" {
		return ""
	}
	// Compute number of replacements.
	m := strings.Count(s, "_")
	if m == 0 {
		return string(unicode.ToUpper(rune(s[0]))) + s[1:] // avoid allocation
	}

	// Apply replacements to buffer.
	l := len(s) - m
	if l == 0 {
		return ""
	}
	t := make([]byte, l)

	w := 0
	start := 0
	for i := 0; i < m; i++ {
		j := start
		j += strings.Index(s[start:], "_")
		if start != j {
			t[w] = byte(unicode.ToUpper(rune(s[start])))
			w++
			w += copy(t[w:], s[start+1:j])
		}
		start = j + 1
	}
	if s[start:] != "" {
		t[w] = byte(unicode.ToUpper(rune(s[start])))
		w++
		w += copy(t[w:], s[start+1:])
	}
	return string(t[0:w])
}
