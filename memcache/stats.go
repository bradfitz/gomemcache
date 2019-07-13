/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http:// www.apache.org/licenses/LICENSE-2.0

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
	"strconv"
	"strings"
	"sync"
)

type errorsSlice []error

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
	//ServerErr Error if can't get the stats information
	ServerErr error
	// Errs contains the errors that occurred while parsing the response
	Errs errorsSlice

	// Version Version string of this server.
	Version string
	// AcceptingConns Whether or not server is accepting conns.
	AcceptingConns bool
	// HashIsExpanding Indicates if the hash table is being grown to a new size.
	HashIsExpanding bool
	// SlabReassignRunning If a slab page is being moved.
	SlabReassignRunning bool
	// PID Process id of this server process.
	PID uint32
	// Uptime Number of secs since the server started.
	Uptime uint32
	// Time current UNIX time according to the server.
	Time uint32
	// RusageUser Accumulated user time for this process (seconds:microseconds).
	RusageUser float64
	// RusageSystem Accumulated system time for this process (seconds:microseconds).
	RusageSystem float64
	// MaxConnections Max number of simultaneous connections.
	MaxConnections uint32
	// CurrConnections Number of open connections.
	CurrConnections uint32
	// TotalConnections Total number of connections opened since the server started running.
	TotalConnections uint32
	// ConnectionStructures Number of connection structures allocated by the server.
	ConnectionStructures uint32
	// ReservedFds Number of misc fds used internally.
	ReservedFds uint32
	// Threads Number of worker threads requested. (see doc/threads.txt).
	Threads uint32
	// HashPowerLevel Current size multiplier for hash table.
	HashPowerLevel uint32
	// SlabGlobalPagePool Slab pages returned to global pool for reassignment to other slab classes.
	SlabGlobalPagePool uint32
	// PointerSize Default size of pointers on the host OS (generally 32 or 64).
	PointerSize uint64
	// CurrItems Current number of items stored.
	CurrItems uint64
	// TotalItems Total number of items stored since the server started.
	TotalItems uint64
	// Bytes Current number of bytes used to store items.
	Bytes uint64
	// RejectedConnections Conns rejected in maxconns_fast mode.
	RejectedConnections uint64
	// CmdGet Cumulative number of retrieval reqs.
	CmdGet uint64
	// CmdSet Cumulative number of storage reqs.
	CmdSet uint64
	// CmdFlush Cumulative number of flush reqs.
	CmdFlush uint64
	// CmdTouch Cumulative number of touch reqs.
	CmdTouch uint64
	// GetHits Number of keys that have been requested and found present.
	GetHits uint64
	// GetMisses Number of items that have been requested and not found.
	GetMisses uint64
	// GetExpired Number of items that have been requested but had already expired.
	GetExpired uint64
	// GetFlushed Number of items that have been requested but have been flushed via flush_all.
	GetFlushed uint64
	// DeleteMisses Number of deletions reqs for missing keys.
	DeleteMisses uint64
	// DeleteHits Number of deletion reqs resulting in an item being removed.
	DeleteHits uint64
	// IncrMisses Number of incr reqs against missing keys.
	IncrMisses uint64
	// IncrHits Number of successful incr reqs.
	IncrHits uint64
	// DecrMisses Number of decr reqs against missing keys.
	DecrMisses uint64
	// DecrHits Number of successful decr reqs.
	DecrHits uint64
	// CasMisses Number of CAS reqs against missing keys.
	CasMisses uint64
	// CasHits Number of successful CAS reqs.
	CasHits uint64
	// CasBadval Number of CAS reqs for which a key was found, but the CAS value did not match.
	CasBadval uint64
	// TouchHits Number of keys that have been touched with a new expiration time.
	TouchHits uint64
	// TouchMisses Number of items that have been touched and not found.
	TouchMisses uint64
	// AuthCmds Number of authentication commands handled, success or failure.
	AuthCmds uint64
	// AuthErrors Number of failed authentications.
	AuthErrors uint64
	// IdleKicks Number of connections closed due to reaching their idle timeout.
	IdleKicks uint64
	// Evictions Number of valid items removed from cache to free memory for new items.
	Evictions uint64
	// Reclaimed Number of times an entry was stored using memory from an expired entry.
	Reclaimed uint64
	// BytesRead Total number of bytes read by this server from network.
	BytesRead uint64
	// BytesWritten Total number of bytes sent by this server to network.
	BytesWritten uint64
	// LimitMaxbytes Number of bytes this server is allowed to use for storage.
	LimitMaxbytes uint64
	// ListenDisabledNum Number of times server has stopped accepting new connections (maxconns).
	ListenDisabledNum uint64
	// TimeInListenDisabledUs Number of microseconds in maxconns.
	TimeInListenDisabledUs uint64
	// ConnYields Number of times any connection yielded to another due to hitting the -R limit.
	ConnYields uint64
	// HashBytes Bytes currently used by hash tables.
	HashBytes uint64
	// ExpiredUnfetched Items pulled from LRU that were never touched by get/incr/append/etc before expiring.
	ExpiredUnfetched uint64
	// EvictedUnfetched Items evicted from LRU that were never touched by get/incr/append/etc.
	EvictedUnfetched uint64
	// EvictedActive Items evicted from LRU that had been hit recently but did not jump to top of LRU.
	EvictedActive uint64
	// SlabsMoved Total slab pages moved.
	SlabsMoved uint64
	// CrawlerReclaimed Total items freed by LRU Crawler.
	CrawlerReclaimed uint64
	// CrawlerItemsChecked Total items examined by LRU Crawler.
	CrawlerItemsChecked uint64
	// LrutailReflocked Times LRU tail was found with active ref. Items can be evicted to avoid OOM errors.
	LrutailReflocked uint64
	// MovesToCold Items moved from HOT/WARM to COLD LRU's.
	MovesToCold uint64
	// MovesToWarm Items moved from COLD to WARM LRU.
	MovesToWarm uint64
	// MovesWithinLru Items reshuffled within HOT or WARM LRU's.
	MovesWithinLru uint64
	// DirectReclaims Times worker threads had to directly reclaim or evict items.
	DirectReclaims uint64
	// LruCrawlerStarts Times an LRU crawler was started.
	LruCrawlerStarts uint64
	// LruMaintainerJuggles Number of times the LRU bg thread woke up.
	LruMaintainerJuggles uint64
	// SlabReassignRescues Items rescued from eviction in page move.
	SlabReassignRescues uint64
	// SlabReassignEvictionsNomem Valid items evicted during a page move (due to no free memory in slab).
	SlabReassignEvictionsNomem uint64
	// SlabReassignChunkRescues Individual sections of an item rescued during a page move.
	SlabReassignChunkRescues uint64
	// SlabReassignInlineReclaim Internal stat counter for when the page mover clears memory from the chunk freelist when it wasn't expecting to.
	SlabReassignInlineReclaim uint64
	// SlabReassignBusyItems Items busy during page move, requiring a retry before page can be moved.
	SlabReassignBusyItems uint64
	// SlabReassignBusyDeletes Items busy during page move, requiring deletion before page can be moved.
	SlabReassignBusyDeletes uint64
	// LogWorkerDropped Logs a worker never wrote due to full buf.
	LogWorkerDropped uint64
	// LogWorkerWritten Logs written by a worker, to be picked up.
	LogWorkerWritten uint64
	// LogWatcherSkipped Logs not sent to slow watchers.
	LogWatcherSkipped uint64
	// LogWatcherSent Logs written to watchers.
	LogWatcherSent uint64
}

// StatsServers returns the general statistics of the servers
// retrieved with the `stats` command
func (c *Client) StatsServers() (servers map[net.Addr]*ServerStats, err error) {
	// Each ServerStats has its own "asociated" sync.Mutex.
	servers = make(map[net.Addr]*ServerStats)
	muxes := make(map[net.Addr]*sync.Mutex)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	// For each server addres a ServerStats, sync.mutex  is created.
	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = new(ServerStats)
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
			c.statsFromAddr(server, mux, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	return
}

func (c *Client) statsFromAddr(server *ServerStats, mux *sync.Mutex, addr net.Addr) {
	var wg sync.WaitGroup
	err := c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}

		// STAT <name> <value>\r\n
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
				server.parseStatValue(string(line), mux)
				wg.Done()
			}()
		}
	})
	wg.Wait()
	server.ServerErr = err
	return
}

func (server *ServerStats) parseStatValue(line string, mux *sync.Mutex) {
	var err error
	tkns := strings.Split(line, " ")
	v := tkns[2][:len(tkns[2])-2]
	mux.Lock()
	defer mux.Unlock()
	switch tkns[1] {
	case "version":
		server.Version = v
	case "accepting_conns":
		server.AcceptingConns = v == "1"
	case "hash_is_expanding":
		server.HashIsExpanding = v == "1"
	case "slab_reassign_running":
		server.SlabReassignRunning = v == "1"
	case "pid":
		server.PID, err = getUtin32Val(v)
	case "uptime":
		server.Uptime, err = getUtin32Val(v)
	case "time":
		server.Time, err = getUtin32Val(v)
	case "rusage_user":
		server.RusageUser, err = getFloat64Val(v)
	case "rusage_system":
		server.RusageSystem, err = getFloat64Val(v)
	case "max_connections":
		server.MaxConnections, err = getUtin32Val(v)
	case "curr_connections":
		server.CurrConnections, err = getUtin32Val(v)
	case "total_connections":
		server.TotalConnections, err = getUtin32Val(v)
	case "connection_structures":
		server.ConnectionStructures, err = getUtin32Val(v)
	case "reserved_fds":
		server.ReservedFds, err = getUtin32Val(v)
	case "threads":
		server.Threads, err = getUtin32Val(v)
	case "hash_power_level":
		server.HashPowerLevel, err = getUtin32Val(v)
	case "slab_global_page_pool":
		server.SlabGlobalPagePool, err = getUtin32Val(v)
	case "pointer_size":
		server.PointerSize, err = getUint64Val(v)
	case "curr_items":
		server.CurrItems, err = getUint64Val(v)
	case "total_items":
		server.TotalItems, err = getUint64Val(v)
	case "bytes":
		server.Bytes, err = getUint64Val(v)
	case "rejected_connections":
		server.RejectedConnections, err = getUint64Val(v)
	case "cmd_get":
		server.CmdGet, err = getUint64Val(v)
	case "cmd_set":
		server.CmdSet, err = getUint64Val(v)
	case "cmd_flush":
		server.CmdFlush, err = getUint64Val(v)
	case "cmd_touch":
		server.CmdTouch, err = getUint64Val(v)
	case "get_hits":
		server.GetHits, err = getUint64Val(v)
	case "get_misses":
		server.GetMisses, err = getUint64Val(v)
	case "get_expired":
		server.GetExpired, err = getUint64Val(v)
	case "get_flushed":
		server.GetFlushed, err = getUint64Val(v)
	case "delete_misses":
		server.DeleteMisses, err = getUint64Val(v)
	case "delete_hits":
		server.DeleteHits, err = getUint64Val(v)
	case "incr_misses":
		server.IncrMisses, err = getUint64Val(v)
	case "incr_hits":
		server.IncrHits, err = getUint64Val(v)
	case "decr_misses":
		server.DecrMisses, err = getUint64Val(v)
	case "decr_hits":
		server.DecrHits, err = getUint64Val(v)
	case "cas_misses":
		server.CasMisses, err = getUint64Val(v)
	case "cas_hits":
		server.CasHits, err = getUint64Val(v)
	case "cas_badval":
		server.CasBadval, err = getUint64Val(v)
	case "touch_hits":
		server.TouchHits, err = getUint64Val(v)
	case "touch_misses":
		server.TouchMisses, err = getUint64Val(v)
	case "auth_cmds":
		server.AuthCmds, err = getUint64Val(v)
	case "auth_errors":
		server.AuthErrors, err = getUint64Val(v)
	case "idle_kicks":
		server.IdleKicks, err = getUint64Val(v)
	case "evictions":
		server.Evictions, err = getUint64Val(v)
	case "reclaimed":
		server.Reclaimed, err = getUint64Val(v)
	case "bytes_read":
		server.BytesRead, err = getUint64Val(v)
	case "bytes_written":
		server.BytesWritten, err = getUint64Val(v)
	case "limit_maxbytes":
		server.LimitMaxbytes, err = getUint64Val(v)
	case "listen_disabled_num":
		server.ListenDisabledNum, err = getUint64Val(v)
	case "time_in_listen_disabled_us":
		server.TimeInListenDisabledUs, err = getUint64Val(v)
	case "conn_yields":
		server.ConnYields, err = getUint64Val(v)
	case "hash_bytes":
		server.HashBytes, err = getUint64Val(v)
	case "expired_unfetched":
		server.ExpiredUnfetched, err = getUint64Val(v)
	case "evicted_unfetched":
		server.EvictedUnfetched, err = getUint64Val(v)
	case "evicted_active":
		server.EvictedActive, err = getUint64Val(v)
	case "slabs_moved":
		server.SlabsMoved, err = getUint64Val(v)
	case "crawler_reclaimed":
		server.CrawlerReclaimed, err = getUint64Val(v)
	case "crawler_items_checked":
		server.CrawlerItemsChecked, err = getUint64Val(v)
	case "lrutail_reflocked":
		server.LrutailReflocked, err = getUint64Val(v)
	case "moves_to_cold":
		server.MovesToCold, err = getUint64Val(v)
	case "moves_to_warm":
		server.MovesToWarm, err = getUint64Val(v)
	case "moves_within_lru":
		server.MovesWithinLru, err = getUint64Val(v)
	case "direct_reclaims":
		server.DirectReclaims, err = getUint64Val(v)
	case "lru_crawler_starts":
		server.LruCrawlerStarts, err = getUint64Val(v)
	case "lru_maintainer_juggles":
		server.LruMaintainerJuggles, err = getUint64Val(v)
	case "slab_reassign_rescues":
		server.SlabReassignRescues, err = getUint64Val(v)
	case "slab_reassign_evictions_nomem":
		server.SlabReassignEvictionsNomem, err = getUint64Val(v)
	case "slab_reassign_chunk_rescues":
		server.SlabReassignChunkRescues, err = getUint64Val(v)
	case "slab_reassign_inline_reclaim":
		server.SlabReassignInlineReclaim, err = getUint64Val(v)
	case "slab_reassign_busy_items":
		server.SlabReassignBusyItems, err = getUint64Val(v)
	case "slab_reassign_busy_deletes":
		server.SlabReassignBusyDeletes, err = getUint64Val(v)
	case "log_worker_dropped":
		server.LogWorkerDropped, err = getUint64Val(v)
	case "log_worker_written":
		server.LogWorkerWritten, err = getUint64Val(v)
	case "log_watcher_skipped":
		server.LogWatcherSkipped, err = getUint64Val(v)
	case "log_watcher_sent":
		server.LogWatcherSent, err = getUint64Val(v)
	}
	if err != nil {
		err = errors.New("Unable to parse value for " + tkns[1] + " stat " + err.Error())
		server.Errs.AppendError(err)
	}

}

func getUtin32Val(v string) (uint32, error) {
	i, err := strconv.ParseUint(v, 10, 64)
	return uint32(i), err
}
func getUint64Val(v string) (uint64, error) {
	return strconv.ParseUint(v, 10, 64)
}

func getFloat64Val(v string) (float64, error) {
	return strconv.ParseFloat(v, 64)
}
