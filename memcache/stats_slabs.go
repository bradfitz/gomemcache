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
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
)

// StatsSlabsServer information broken down by slab about items stored in memcached,
// more centered to performance of a slab rather than counts of particular items.
type StatsSlabsServer struct {
	// ServerErr error if can't get the stats information.
	ServerErr error
	// StatsSlabs statistics for each slab.
	StatsSlabs map[string]*StatsSlab
	// ActiveSlabs total number of slab classes allocated.
	ActiveSlabs uint64
	// TotalMalloced total amount of memory allocated to slab pages.
	TotalMalloced uint64
}

// StatsSlab statistics for one slab.
type StatsSlab struct {
	// Errs contains the errors that occurred while parsing the response.
	Errs errorsSlice
	// UnknownStats contains the stats that are not in the struct. This can be useful if the memcached developers add new stats in newer versions.
	UnknownStats map[string]string

	// ChunkSize the amount of space each chunk uses. One item will use one chunk of the appropriate size.
	ChunkSize uint64
	// ChunksPerPage how many chunks exist within one page. A page by default is less than or equal to one megabyte in size. Slabs are allocated by page, then broken into chunks.
	ChunksPerPage uint64
	// TotalPages total number of pages allocated to the slab class.
	TotalPages uint64
	// TotalChunks total number of chunks allocated to the slab class.
	TotalChunks uint64
	// GetHits total number of get requests serviced by this class.
	GetHits uint64
	// CmdSet total number of set requests storing data in this class.
	CmdSet uint64
	// DeleteHits total number of successful deletes from this class.
	DeleteHits uint64
	// IncrHits total number of incrs modifying this class.
	IncrHits uint64
	// DecrHits total number of decrs modifying this class.
	DecrHits uint64
	// CasHits total number of CAS commands modifying this class.
	CasHits uint64
	// CasBadval total number of CAS commands that failed to modify a value due to a bad CAS id.
	CasBadval uint64
	// TouchHits total number of touches serviced by this class.
	TouchHits uint64
	// UsedChunks how many chunks have been allocated to items.
	UsedChunks uint64
	// FreeChunks chunks not yet allocated to items, or freed via delete.
	FreeChunks uint64
	// FreeChunksEnd number of free chunks at the end of the last allocated page.
	FreeChunksEnd uint64
	// MemRequested number of bytes requested to be stored in this slab[*].
	MemRequested uint64
	// ActiveSlabs total number of slab classes allocated.
	ActiveSlabs uint64
	// TotalMalloced total amount of memory allocated to slab pages.
	TotalMalloced uint64
}

// StatsSlabsServers returns information about the storage per slab class of the servers,
// retrieved with the `stats slabs` command.
func (c *Client) StatsSlabsServers() (servers map[net.Addr]*StatsSlabsServer, err error) {
	servers = make(map[net.Addr]*StatsSlabsServer)

	// addrs slice of the all the server adresses from the selector.
	addrs := make([]net.Addr, 0)

	err = c.selector.Each(
		func(addr net.Addr) error {
			addrs = append(addrs, addr)
			servers[addr] = &StatsSlabsServer{StatsSlabs: make(map[string]*StatsSlab)}
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
			c.statsSlabsFromAddr(server, addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	return
}

// From the protocol definition for the command stats items:
//
// Slab statistics
// ---------------
// CAVEAT: This section describes statistics which are subject to change in the
// future.
//
// The "stats" command with the argument of "slabs" returns information about
// each of the slabs created by memcached during runtime. This includes per-slab
// information along with some totals. The data is returned in the format:
//
// STAT <slabclass>:<stat> <value>\r\n
// STAT <stat> <value>\r\n
//
// The server terminates this list with the line:
//
// END\r\n
func (c *Client) statsSlabsFromAddr(sss *StatsSlabsServer, addr net.Addr) (err error) {
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
			// STAT <slabclass>:<stat> <value>\r\n
			// STAT <stat> <value>\r\n
			sss.parseStat(line[5 : len(line)-2])
		}
	})
	sss.ServerErr = err
	return
}

func (sss *StatsSlabsServer) parseStat(line []byte) {
	// <slabclass>:<stat> <value>
	// <stat> <value>
	tkns := bytes.FieldsFunc(line, func(r rune) bool {
		return r == ':' || r == ' '
	})
	if len(tkns) == 2 {
		if string(tkns[0]) == "active_slabs" {
			sss.ActiveSlabs, _ = strconv.ParseUint(string(tkns[1]), 10, 64)
		} else {
			sss.TotalMalloced, _ = strconv.ParseUint(string(tkns[1]), 10, 64)
		}
		return
	}

	slabclass := string(tkns[0])
	// <slabclass> <stat> <value>
	slab := sss.StatsSlabs[slabclass]
	if slab == nil {
		slab = new(StatsSlab)
		slab.UnknownStats = make(map[string]string)
		sss.StatsSlabs[slabclass] = slab
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
