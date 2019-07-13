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
	"strconv"
	"strings"
	"sync"
)

//StatsSlabsServer returns information,broken down by slab about items stored in memcached
// More centered to performance of a slab rather than counts of particular items.
type StatsSlabsServer struct {
	// ServerErr Error if can't get the stats information.
	ServerErr error
	// StatsSlabs statistics for each slab.
	StatsSlabs map[string]*StatsSlab
	//ActiveSlabs Total number of slab classes allocated.
	ActiveSlabs uint64
	//TotalMalloced Total amount of memory allocated to slab pages.
	TotalMalloced uint64
}

//StatsSlab statistics for one slab `
type StatsSlab struct {
	// Errs contains the errors that occurred while parsing the response
	Errs errorsSlice
	// ChunkSize The amount of space each chunk uses. One item will use one chunk of the appropriate size.
	ChunkSize uint64
	// ChunksPerPage How many chunks exist within one page. A page by default is less than or equal to one megabyte in size. Slabs are allocated by page, then broken into chunks.
	ChunksPerPage uint64
	// TotalPages Total number of pages allocated to the slab class.
	TotalPages uint64
	// TotalChunks Total number of chunks allocated to the slab class.
	TotalChunks uint64
	// GetHits Total number of get requests serviced by this class.
	GetHits uint64
	// CmdSet Total number of set requests storing data in this class.
	CmdSet uint64
	// DeleteHits Total number of successful deletes from this class.
	DeleteHits uint64
	// IncrHits Total number of incrs modifying this class.
	IncrHits uint64
	// DecrHits Total number of decrs modifying this class.
	DecrHits uint64
	// CasHits Total number of CAS commands modifying this class.
	CasHits uint64
	// CasBadval Total number of CAS commands that failed to modify a value due to a bad CAS id.
	CasBadval uint64
	// TouchHits Total number of touches serviced by this class.
	TouchHits uint64
	// UsedChunks How many chunks have been allocated to items.
	UsedChunks uint64
	// FreeChunks Chunks not yet allocated to items, or freed via delete.
	FreeChunks uint64
	// FreeChunksEnd Number of free chunks at the end of the last allocated page.
	FreeChunksEnd uint64
	// MemRequested Number of bytes requested to be stored in this slab[*].
	MemRequested uint64
	// ActiveSlabs Total number of slab classes allocated.
	ActiveSlabs uint64
	// TotalMalloced Total amount of memory allocated to slab pages.
	TotalMalloced uint64
}

//StatsSlabsServers returns information about the storage per slab class of the servers,
// retrieved with the `stats slabs` command
func (c *Client) StatsSlabsServers() (servers map[net.Addr]*StatsSlabsServer, err error) {
	// Each ServerStats has its own "asociated" sync.Mutex.
	servers = make(map[net.Addr]*StatsSlabsServer)
	muxes := make(map[net.Addr]*sync.Mutex)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	// For each server addres a StatsSlabsServer, sync.mutex is created.
	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = &StatsSlabsServer{StatsSlabs: make(map[string]*StatsSlab)}
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
			c.statsSlabsFromAddr(server, mux, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	return
}

//From the protocol definition for the command stats items
//
//Slab statistics
//---------------
//CAVEAT: This section describes statistics which are subject to change in the
//future.
//
//The "stats" command with the argument of "slabs" returns information about
//each of the slabs created by memcached during runtime. This includes per-slab
//information along with some totals. The data is returned in the format:
//
//STAT <slabclass>:<stat> <value>\r\n
//STAT <stat> <value>\r\n
//
//The server terminates this list with the line
//
//END\r\n
func (c *Client) statsSlabsFromAddr(sss *StatsSlabsServer, mux *sync.Mutex, addr net.Addr) (err error) {
	var wg sync.WaitGroup
	err = c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "stats slabs\r\n"); err != nil {
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
				return err
			}
			//STAT <slabclass>:<stat> <value>\r\n
			//STAT <stat> <value>\r\n
			wg.Add(1)
			go func() {
				sss.parseStat(string(line[5:len(line)-2]), mux)
				wg.Done()
			}()
		}
	})
	wg.Wait()
	sss.ServerErr = err
	return
}

func (sss *StatsSlabsServer) parseStat(line string, mux *sync.Mutex) {
	//<stat> <value>
	tkns := strings.Split(string(line), ":")
	if len(tkns) == 1 {
		tkns := strings.Split(string(line), " ")
		mux.Lock()
		if tkns[0] == "active_slabs" {
			sss.ActiveSlabs, _ = strconv.ParseUint(tkns[1], 10, 64)
		} else {
			sss.TotalMalloced, _ = strconv.ParseUint(tkns[1], 10, 64)
		}
		mux.Unlock()
		return
	}

	//<slabclass>:<stat> <value>

	statValue := strings.Split(tkns[1], " ")
	mux.Lock()
	defer mux.Unlock()
	slab := sss.StatsSlabs[tkns[0]]
	if slab == nil {
		slab = new(StatsSlab)
		sss.StatsSlabs[tkns[0]] = slab
	}
	slab.setStatValue(statValue[0], statValue[1])
}

func (slab *StatsSlab) setStatValue(stat, value string) {
	v, err := getUint64Val(value)
	if err != nil {
		err = errors.New("Unable to parse value  for stat" + stat + " stat " + err.Error())
		slab.Errs.AppendError(err)
		return
	}
	switch stat {
	case "chunk_size":
		slab.ChunkSize = v
	case "chunks_per_page":
		slab.ChunksPerPage = v
	case "total_pages":
		slab.TotalPages = v
	case "total_chunks":
		slab.TotalChunks = v
	case "get_hits":
		slab.GetHits = v
	case "cmd_set":
		slab.CmdSet = v
	case "delete_hits":
		slab.DeleteHits = v
	case "incr_hits":
		slab.IncrHits = v
	case "decr_hits":
		slab.DecrHits = v
	case "cas_hits":
		slab.CasHits = v
	case "cas_badval":
		slab.CasBadval = v
	case "touch_hits":
		slab.TouchHits = v
	case "used_chunks":
		slab.UsedChunks = v
	case "free_chunks":
		slab.FreeChunks = v
	case "free_chunks_end":
		slab.FreeChunksEnd = v
	case "mem_requested":
		slab.MemRequested = v
	case "active_slabs":
		slab.ActiveSlabs = v
	case "total_malloced":
		slab.TotalMalloced = v
	}
}
