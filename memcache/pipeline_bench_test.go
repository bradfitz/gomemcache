/*
Copyright 2026 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0
*/

package memcache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// BenchmarkPipelining measures wall-clock time for a burst of N concurrent
// Gets over a single backend with a fixed per-direction delay, comparing
// pipelined vs MaxPipelineDepth=1 (non-pipelined). With MaxConns bounded,
// non-pipelined mode must serialize ops across a few conns while pipelined
// mode keeps the pipe saturated; the gap grows with RTT and N.
//
// This benchmark uses real time (no synctest), so RTTs are kept small to
// keep total wall time reasonable. It still exhibits the expected relative
// behavior because pipelining turns N sequential RTTs into one.
func BenchmarkPipelining(b *testing.B) {
	for _, rtt := range []time.Duration{
		100 * time.Microsecond,
		500 * time.Microsecond,
		2 * time.Millisecond,
	} {
		for _, n := range []int{10, 100} {
			for _, pipelined := range []bool{true, false} {
				name := fmt.Sprintf("rtt=%s/n=%d/pipe=%v", rtt, n, pipelined)
				b.Run(name, func(b *testing.B) {
					benchPipelining(b, rtt, n, pipelined)
				})
			}
		}
	}
}

func benchPipelining(b *testing.B, rtt time.Duration, n int, pipelined bool) {
	b.Helper()
	ln := newFakeListener("mc-bench", rtt/2)
	srv := &testServer{}
	go srv.Serve(ln)
	defer ln.Close()

	c := NewFromSelector(singleServerSelector{addr: ln.Addr()})
	c.Timeout = 10 * time.Second
	if !pipelined {
		c.MaxPipelineDepth = 1
	}
	c.MaxConns = 4
	c.MaxDials = 4
	c.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return ln.dial()
	}
	defer c.Close()

	if err := c.Set(&Item{Key: "k", Value: []byte("v")}); err != nil {
		b.Fatalf("seed Set: %v", err)
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := c.Get("k"); err != nil {
					b.Errorf("Get: %v", err)
				}
			}()
		}
		wg.Wait()
	}
}
