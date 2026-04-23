/*
Copyright 2026 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0
*/

package memcache

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// This file provides a minimal in-memory net.Conn pair with per-direction
// synthetic delay.
//
// Under testing/synctest, time.Sleep and time.AfterFunc use fake time, so
// the "delay" is deterministic and instant in wall-clock terms.

type fakeAddr string

func (a fakeAddr) Network() string { return "fakenet" }
func (a fakeAddr) String() string  { return string(a) }

// newFakePipe returns two connected fakeConns. oneWayDelay models link
// latency (the time between Write on one side and the bytes becoming
// readable on the other). A Write records its target arrival time when it
// queues the chunk; the per-direction worker then sleeps to each arrival
// time in order. Under back-to-back Writes, chunks arrive back-to-back,
// emulating an in-flight pipe rather than a queue of serialized latencies.
func newFakePipe(oneWayDelay time.Duration, nameA, nameB string) (a, b *fakeConn) {
	a = &fakeConn{
		local:  fakeAddr(nameA),
		remote: fakeAddr(nameB),
		delay:  oneWayDelay,
		done:   make(chan struct{}),
		sendCh: make(chan fakeChunk, 256),
	}
	b = &fakeConn{
		local:  fakeAddr(nameB),
		remote: fakeAddr(nameA),
		delay:  oneWayDelay,
		done:   make(chan struct{}),
		sendCh: make(chan fakeChunk, 256),
	}
	aToB := make(chan []byte, 256)
	bToA := make(chan []byte, 256)
	a.outCh = aToB
	b.inCh = aToB
	b.outCh = bToA
	a.inCh = bToA
	a.peer = b
	b.peer = a
	go a.deliverLoop()
	go b.deliverLoop()
	return
}

type fakeChunk struct {
	arriveAt time.Time
	data     []byte
}

type fakeConn struct {
	local  fakeAddr
	remote fakeAddr
	delay  time.Duration

	peer *fakeConn

	sendCh chan fakeChunk // Write pushes here with arrival timestamp
	outCh  chan []byte    // deliverLoop writes here when arrival time passes (peer reads)
	inCh   chan []byte    // chunks arriving from peer (already delayed)

	closeOnce sync.Once
	done      chan struct{}

	mu            sync.Mutex
	readBuf       []byte
	readDeadline  time.Time
	writeDeadline time.Time
}

// deliverLoop pops chunks in FIFO order and sleeps until each chunk's
// arrival time. Arrival times are monotonic because Write stamps them at
// submission time on a single-writer send queue.
func (c *fakeConn) deliverLoop() {
	for {
		select {
		case ch, ok := <-c.sendCh:
			if !ok {
				return
			}
			if d := time.Until(ch.arriveAt); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-timer.C:
				case <-c.done:
					timer.Stop()
					return
				case <-c.peer.done:
					timer.Stop()
					return
				}
			}
			select {
			case c.outCh <- ch.data:
			case <-c.done:
				return
			case <-c.peer.done:
				return
			}
		case <-c.done:
			return
		case <-c.peer.done:
			return
		}
	}
}

func (c *fakeConn) LocalAddr() net.Addr  { return c.local }
func (c *fakeConn) RemoteAddr() net.Addr { return c.remote }

func (c *fakeConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

func (c *fakeConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *fakeConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
	})
	return nil
}

// Read returns data from the per-conn read buffer, refilling it from inCh
// when empty. Respects the read deadline (via time.Timer, fake-time-friendly).
func (c *fakeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.readBuf) > 0 {
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		c.mu.Unlock()
		return n, nil
	}
	dl := c.readDeadline
	c.mu.Unlock()

	var dlCh <-chan time.Time
	if !dl.IsZero() {
		timer := time.NewTimer(time.Until(dl))
		defer timer.Stop()
		dlCh = timer.C
	}

	select {
	case chunk, ok := <-c.inCh:
		if !ok {
			return 0, io.EOF
		}
		c.mu.Lock()
		c.readBuf = append(c.readBuf, chunk...)
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		c.mu.Unlock()
		return n, nil
	case <-dlCh:
		return 0, errFakeTimeout
	case <-c.done:
		return 0, io.EOF
	case <-c.peer.done:
		return 0, io.EOF
	}
}

// Write copies the bytes, stamps the link arrival time, and queues for
// delivery. Chunks arrive at the peer at (send_time + delay); back-to-back
// Writes result in back-to-back arrivals, not serialized ones.
func (c *fakeConn) Write(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, io.ErrClosedPipe
	case <-c.peer.done:
		return 0, io.ErrClosedPipe
	default:
	}
	b := make([]byte, len(p))
	copy(b, p)
	ch := fakeChunk{arriveAt: time.Now().Add(c.delay), data: b}
	select {
	case c.sendCh <- ch:
	case <-c.done:
		return 0, io.ErrClosedPipe
	case <-c.peer.done:
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

var errFakeTimeout = &fakeTimeoutError{}

type fakeTimeoutError struct{}

func (*fakeTimeoutError) Error() string   { return "fakenet: i/o timeout" }
func (*fakeTimeoutError) Timeout() bool   { return true }
func (*fakeTimeoutError) Temporary() bool { return true }

// fakeListener is a net.Listener backed by fakeConn pairs. Each dial creates
// one pair; one end is returned to the dialer, the other end is enqueued for
// the next Accept. delay is the per-direction one-way delay.
type fakeListener struct {
	addr    fakeAddr
	delay   time.Duration
	accepts chan net.Conn
	closed  chan struct{}
	once    sync.Once
}

func newFakeListener(addr string, oneWayDelay time.Duration) *fakeListener {
	return &fakeListener{
		addr:    fakeAddr(addr),
		delay:   oneWayDelay,
		accepts: make(chan net.Conn, 16),
		closed:  make(chan struct{}),
	}
}

// dial creates a new conn pair and hands the server side to the next Accept.
// It returns the client side.
func (l *fakeListener) dial() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, errors.New("fakenet: listener closed")
	default:
	}
	client, server := newFakePipe(l.delay, "client-"+string(l.addr), string(l.addr))
	select {
	case l.accepts <- server:
		return client, nil
	case <-l.closed:
		client.Close()
		server.Close()
		return nil, errors.New("fakenet: listener closed")
	}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accepts:
		return c, nil
	case <-l.closed:
		return nil, errors.New("fakenet: listener closed")
	}
}

func (l *fakeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *fakeListener) Addr() net.Addr { return l.addr }

// singleServerSelector is a ServerSelector that always picks the same addr.
type singleServerSelector struct{ addr net.Addr }

func (s singleServerSelector) PickServer(key string) (net.Addr, error) { return s.addr, nil }
func (s singleServerSelector) Each(f func(net.Addr) error) error       { return f(s.addr) }

// Compile-time assertions.
var (
	_ net.Conn       = (*fakeConn)(nil)
	_ net.Addr       = fakeAddr("")
	_ error          = (*fakeTimeoutError)(nil)
	_ net.Listener   = (*fakeListener)(nil)
	_ ServerSelector = singleServerSelector{}
)
