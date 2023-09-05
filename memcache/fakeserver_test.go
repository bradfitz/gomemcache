/*
Copyright 2023 The gomemcache AUTHORS

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
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type testServer struct {
	mu      sync.Mutex
	m       map[string]serverItem
	nextCas uint64
}

type serverItem struct {
	flags   uint32
	data    []byte
	exp     time.Time // or zero value for no expiry
	casUniq uint64
}

func (s *testServer) Serve(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		tc := &testConn{s: s, c: c}
		go tc.serve()
	}
}

type testConn struct {
	s  *testServer
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer
}

func (c *testConn) serve() {
	defer c.c.Close()
	c.br = bufio.NewReader(c.c)
	c.bw = bufio.NewWriter(c.c)
	for {
		line, err := c.br.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			return
		}
		if !c.handleRequestLine(string(line)) {
			panic(fmt.Sprintf("unhandled request line in testServer: %q", line))
		}
	}
}

func (c *testConn) reply(msg string) bool {
	fmt.Fprintf(c.bw, "%s\r\n", msg)
	c.bw.Flush()
	return true
}

var (
	writeRx    = regexp.MustCompile(`^(set|add|replace|append|prepend|cas) (\S+) (\d+) (\d+) (\d+)(?: (\S+))?( noreply)?\r\n`)
	deleteRx   = regexp.MustCompile(`^delete (\S+)( noreply)?\r\n`)
	incrDecrRx = regexp.MustCompile(`^(incr|decr) (\S+) (\d+)( noreply)?\r\n`)
	touchRx    = regexp.MustCompile(`^touch (\S+) (\d+)( noreply)?\r\n`)
)

func (c *testConn) handleRequestLine(line string) bool {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()

	switch line {
	case "quit\r\n":
		return false
	case "version\r\n":
		return c.reply("VERSION go-client-unit-test")
	case "flush_all\r\n":
		c.s.m = make(map[string]serverItem)
		return c.reply("OK")
	}

	if strings.HasPrefix(line, "gets ") {
		keys := strings.Fields(strings.TrimPrefix(line, "gets "))
		for _, key := range keys {
			item, ok := c.s.m[key]
			if !ok {
				continue
			}
			if !item.exp.IsZero() && item.exp.Before(time.Now()) {
				delete(c.s.m, key)
				continue
			}
			fmt.Fprintf(c.bw, "VALUE %s %d %d %d\r\n", key, item.flags, len(item.data), item.casUniq)
			c.bw.Write(item.data)
			c.bw.Write(crlf)
		}
		return c.reply("END")
	}

	if m := deleteRx.FindStringSubmatch(line); m != nil {
		key, noReply := m[1], strings.TrimSpace(m[2])
		len0 := len(c.s.m)
		delete(c.s.m, key)
		len1 := len(c.s.m)
		if noReply == "" {
			if len0 == len1 {
				return c.reply("NOT_FOUND")
			}
			return c.reply("DELETED")
		}
		return true
	}

	if m := touchRx.FindStringSubmatch(line); m != nil {
		key, exptimeStr, noReply := m[1], m[2], strings.TrimSpace(m[3])
		exptimeVal, _ := strconv.ParseInt(exptimeStr, 10, 64)

		item, ok := c.s.m[key]
		if ok {
			item.exp = computeExpTime(exptimeVal)
			c.s.m[key] = item
		}
		if noReply == "" {
			if ok {
				return c.reply("TOUCHED")
			} else {
				return c.reply("NOT_FOUND")
			}
		}
		return true
	}

	if m := writeRx.FindStringSubmatch(line); m != nil {
		verb, key, flagsStr, exptimeStr, lenStr, casUniq, noReply := m[1], m[2], m[3], m[4], m[5], m[6], strings.TrimSpace(m[7])
		flags, _ := strconv.ParseUint(flagsStr, 10, 32)
		exptimeVal, _ := strconv.ParseInt(exptimeStr, 10, 64)
		itemLen, _ := strconv.ParseInt(lenStr, 10, 32)
		//log.Printf("got %q flags=%q exp=%d %d len=%d cas=%q noreply=%q", verb, key, flags, exptimeVal, itemLen, casUniq, noReply)
		if c.s.m == nil {
			c.s.m = make(map[string]serverItem)
		}
		reply := func(msg string) bool {
			if noReply != "noreply" {
				c.reply(msg)
			}
			return true
		}
		body := make([]byte, itemLen+2)
		if _, err := io.ReadFull(c.br, body); err != nil {
			log.Printf("error reading %q body for key %q: %v", verb, key, err)
			return false
		}
		if !bytes.HasSuffix(body, []byte("\r\n")) {
			log.Printf("missing \\r\\n suffix for %q body for key %q", verb, key)
			return false
		}

		was, ok := c.s.m[key]
		if ok && (was.exp.After(time.Now()) || exptimeVal < 0) {
			delete(c.s.m, key)
			ok = false
		}
		c.s.nextCas++
		newItem := serverItem{
			flags:   uint32(flags),
			data:    body[:itemLen],
			casUniq: c.s.nextCas,
			exp:     computeExpTime(exptimeVal),
		}
		switch verb {
		case "set":
			c.s.m[key] = newItem
			return reply("STORED")
		case "add":
			if ok {
				return reply("NOT_STORED")
			}
			c.s.m[key] = newItem
			return reply("STORED")
		case "replace":
			if !ok {
				return reply("NOT_STORED")
			}
			c.s.m[key] = newItem
			return reply("STORED")
		case "cas":
			if !ok {
				reply("NOT_FOUND")
			}
			if casUniq != fmt.Sprint(was.casUniq) {
				return reply("EXISTS")
			}
			c.s.m[key] = newItem
			return reply("STORED")
		case "append":
			if !ok {
				return reply("NOT_STORED")
			}
			newItem.data = bytes.Join([][]byte{was.data, newItem.data}, nil)
			c.s.m[key] = newItem
			return reply("STORED")
		case "prepend":
			if !ok {
				return reply("NOT_STORED")
			}
			newItem.data = bytes.Join([][]byte{newItem.data, was.data}, nil)
			c.s.m[key] = newItem
			return reply("STORED")
		}
	}

	if m := incrDecrRx.FindStringSubmatch(line); m != nil {
		verb, key, deltaStr, noReply := m[1], m[2], m[3], strings.TrimSpace(m[4])
		delta, _ := strconv.ParseInt(deltaStr, 10, 64)
		reply := func(msg string) bool {
			if noReply != "noreply" {
				c.reply(msg)
			}
			return true
		}
		item, ok := c.s.m[key]
		if !ok {
			return reply("NOT_FOUND")
		}
		oldVal, err := strconv.ParseInt(string(item.data), 10, 64)
		if err != nil {
			return reply("CLIENT_ERROR cannot increment or decrement non-numeric value")
		}
		var newVal int64
		if verb == "decr" {
			if delta < oldVal {
				newVal = oldVal - delta
			} else {
				newVal = 0
			}
		} else {
			newVal = oldVal + delta
		}
		item.data = []byte(strconv.FormatInt(newVal, 10))
		c.s.m[key] = item
		if noReply == "" {
			fmt.Fprintf(c.bw, "%d\r\n", newVal)
			c.bw.Flush()
		}
		return true
	}

	return false

}

func computeExpTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	if n <= 60*60*24*30 {
		return time.Now().Add(time.Duration(n) * time.Second)
	}
	return time.Unix(n, 0)
}
