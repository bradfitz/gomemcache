/*
Copyright 2026 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0
*/

package memcache

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultMaxConns is the default value for Client.MaxConns.
	DefaultMaxConns = 100

	// DefaultMaxDials is the default value for Client.MaxDials.
	DefaultMaxDials = 10
)

// defaultMaxPipelineDepth is the per-conn pipeline depth used when
// Client.MaxPipelineDepth is zero. The scheduler caps each conn's in-flight
// count at the effective MaxPipelineDepth; once a conn reaches the cap it
// prefers to grow (dial a new conn) subject to MaxConns and MaxDials.
const defaultMaxPipelineDepth = 8

// maxAttempts caps retries of idempotent requests across dead conns.
const maxAttempts = 2

var errBackendClosed = errors.New("memcache: backend closed")

// set is a tiny generic set helper used internally.
type set[T comparable] map[T]struct{}

func (s set[T]) add(v T)    { s[v] = struct{}{} }
func (s set[T]) remove(v T) { delete(s, v) }

// idempotentVerb reports whether a memcached ASCII verb is safe to retry
// after a connection failure of undetermined phase.
func idempotentVerb(verb string) bool {
	switch verb {
	case "get", "gets", "gat", "gats",
		"set", "delete", "touch", "flush_all", "version":
		return true
	}
	// add, replace, cas, append, prepend, incr, decr are stateful.
	return false
}

// pipeReq is one pipelined operation.
//
// write is invoked by the conn's writer goroutine when this req is at the
// head of the write queue; it formats the request directly onto the
// bufio.Writer. read is invoked by the reader goroutine when the
// corresponding response is at the head of the response FIFO.
type pipeReq struct {
	write      func(w *bufio.Writer) error
	read       func(r *bufio.Reader) error
	verb       string // for idempotent classification
	idempotent bool
	attempts   int32 // scheduler-only
	cancelled  int32 // atomic; 0 or 1
	done       chan error
}

func (r *pipeReq) isCancelled() bool      { return atomic.LoadInt32(&r.cancelled) != 0 }
func (r *pipeReq) markCancelled()         { atomic.StoreInt32(&r.cancelled, 1) }

// newPipeReq constructs a pipeReq.
func newPipeReq(verb string, write func(w *bufio.Writer) error, read func(r *bufio.Reader) error) *pipeReq {
	return &pipeReq{
		write:      write,
		read:       read,
		verb:       verb,
		idempotent: idempotentVerb(verb),
		done:       make(chan error, 1),
	}
}

// dialResult carries the outcome of a single dial attempt to the scheduler.
type dialResult struct {
	cn  *pipeConn
	err error
}

// backend is the per-address connection manager.
//
// All mutable state (conns, queue, dialing) is owned by the scheduler
// goroutine run(). Callers communicate only through channels, so blocking
// operations compose with ctx.Done() for future context-aware APIs.
type backend struct {
	c    *Client
	addr net.Addr

	incoming chan *pipeReq
	dialDone chan dialResult
	connLost chan *pipeConn
	connIdle chan struct{} // non-blocking; kicks scheduler to drain queue after an op completes
	reapIdle chan struct{} // non-blocking; idleWatch asks scheduler to reap idle conns
	quit     chan struct{} // closed on shutdown
	runDone  chan struct{} // closed when run() returns

	// Idle-reaper state (see idleWatch).
	lastUsedUnixNano int64 // atomic; updated on every submit
	idleTimerActive  int32 // atomic; 0 = no idleWatch goroutine, 1 = running

	// Scheduler-owned state (only touched inside run()).
	conns   set[*pipeConn]
	queue   []*pipeReq
	dialing int
}

func newBackend(c *Client, addr net.Addr) *backend {
	b := &backend{
		c:        c,
		addr:     addr,
		incoming: make(chan *pipeReq),
		dialDone: make(chan dialResult, 16),
		connLost: make(chan *pipeConn, 16),
		connIdle: make(chan struct{}, 1),
		reapIdle: make(chan struct{}, 1),
		quit:     make(chan struct{}),
		runDone:  make(chan struct{}),
		conns:    set[*pipeConn]{},
	}
	go b.run()
	return b
}

func (b *backend) maxConns() int {
	m := b.c.MaxConns
	switch {
	case m < 0:
		return -1
	case m == 0:
		return DefaultMaxConns
	default:
		return m
	}
}

func (b *backend) maxDials() int {
	m := b.c.MaxDials
	switch {
	case m < 0:
		return -1
	case m == 0:
		return DefaultMaxDials
	default:
		return m
	}
}

// maxDepth returns the effective per-conn pipeline depth cap.
func (b *backend) maxDepth() int32 {
	m := b.c.MaxPipelineDepth
	if m <= 0 {
		return defaultMaxPipelineDepth
	}
	return int32(m)
}

// submit sends req to the backend, blocks until done or ctx is cancelled.
// ctx is accepted as context.Background() today; a public ctx-aware API can
// plumb real contexts in later without touching scheduler logic.
func (b *backend) submit(ctx context.Context, req *pipeReq) error {
	atomic.StoreInt64(&b.lastUsedUnixNano, time.Now().UnixNano())
	if atomic.CompareAndSwapInt32(&b.idleTimerActive, 0, 1) {
		go b.idleWatch()
	}
	select {
	case b.incoming <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.quit:
		return errBackendClosed
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		req.markCancelled()
		return ctx.Err()
	}
}

// idleReapIntervalNanos is how often idleWatch checks for inactivity, in
// nanoseconds. After one interval with no submits, surplus idle conns are
// reaped (see doReap). Accessed atomically so tests can shorten it without
// racing the idleWatch goroutines that read it.
var idleReapIntervalNanos int64 = int64(5 * time.Second)

func idleReapInterval() time.Duration {
	return time.Duration(atomic.LoadInt64(&idleReapIntervalNanos))
}

// idleWatch runs once per period-of-activity on a backend. It ticks every
// idleReapInterval; if lastUsedUnixNano hasn't advanced since the previous
// tick, it signals the scheduler to reap excess idle conns, flips
// idleTimerActive back to 0, and exits. The next submit will CAS it back
// to 1 and spawn a fresh idleWatch.
func (b *backend) idleWatch() {
	// Seed observed to the value submit already stored before spawning us.
	observed := atomic.LoadInt64(&b.lastUsedUnixNano)
	for {
		select {
		case <-time.After(idleReapInterval()):
		case <-b.quit:
			atomic.StoreInt32(&b.idleTimerActive, 0)
			return
		}
		cur := atomic.LoadInt64(&b.lastUsedUnixNano)
		if cur == observed {
			select {
			case b.reapIdle <- struct{}{}:
			default:
			}
			atomic.StoreInt32(&b.idleTimerActive, 0)
			return
		}
		observed = cur
	}
}

// close stops the scheduler and all its conns. It is safe to call multiple
// times.
func (b *backend) close() {
	select {
	case <-b.quit:
		return
	default:
	}
	close(b.quit)
	<-b.runDone
}

func (b *backend) run() {
	defer close(b.runDone)
	for {
		select {
		case req := <-b.incoming:
			if req.isCancelled() {
				continue
			}
			b.tryDispatch(req)
		case r := <-b.dialDone:
			b.dialing--
			if r.err == nil {
				b.conns.add(r.cn)
				go r.cn.writer()
				go r.cn.reader()
			}
			// Whether success or failure, try to drain the queue. On
			// failure, tryDispatch will re-queue and may restart a dial.
			b.drainQueueLocked(r.err)
		case cn := <-b.connLost:
			b.handleConnLost(cn)
		case <-b.connIdle:
			b.drainQueueLocked(nil)
		case <-b.reapIdle:
			b.doReap()
		case <-b.quit:
			b.shutdown()
			return
		}
	}
}

// canGrow reports whether the scheduler may start another dial.
func (b *backend) canGrow() bool {
	mc := b.maxConns()
	md := b.maxDials()
	if mc >= 0 && len(b.conns)+b.dialing >= mc {
		return false
	}
	if md >= 0 && b.dialing >= md {
		return false
	}
	return true
}

// tryDispatch assigns req to a conn, queues it, or starts a dial.
// It is called only from run().
func (b *backend) tryDispatch(req *pipeReq) {
	// Pick conn with smallest inFlight.
	var best *pipeConn
	minIF := int32(-1)
	for cn := range b.conns {
		v := atomic.LoadInt32(&cn.inFlight)
		if minIF == -1 || v < minIF {
			minIF = v
			best = cn
		}
	}

	maxDepth := b.maxDepth()
	canGrow := b.canGrow()

	// Dispatch only if some conn has room below the depth cap. With
	// MaxPipelineDepth=1 this means fully-idle conns only, i.e. strict
	// one-op-per-conn serialization (no pipelining).
	if best != nil && minIF < maxDepth {
		atomic.AddInt32(&best.inFlight, 1)
		select {
		case best.reqCh <- req:
			return
		default:
			atomic.AddInt32(&best.inFlight, -1)
		}
	}

	b.queue = append(b.queue, req)

	// Dial only when we're actually behind: if existing dials-in-flight can
	// absorb the queue, don't pile on more. Each future conn is expected to
	// take ~maxDepth reqs before we'd want another. Without this, bursts
	// of 50 concurrent submits trigger ~50 dials instead of ~50/maxDepth.
	if canGrow && len(b.queue) > b.dialing*int(maxDepth) {
		b.startDial()
	}
}

// startDial launches a dial goroutine. It must be called from run().
func (b *backend) startDial() {
	b.dialing++
	go func() {
		nc, err := b.c.dial(b.addr)
		var res dialResult
		if err != nil {
			res = dialResult{err: err}
		} else {
			res = dialResult{cn: newPipeConn(b, nc)}
		}
		select {
		case b.dialDone <- res:
		case <-b.quit:
			if res.cn != nil {
				res.cn.nc.Close()
			}
		}
	}()
}

// drainQueueLocked retries dispatching all queued reqs. If lastDialErr != nil
// and we still can't dispatch and can't grow, fail the queue with that error
// so callers don't hang forever on a dead backend.
func (b *backend) drainQueueLocked(lastDialErr error) {
	q := b.queue
	b.queue = nil
	for _, req := range q {
		if req.isCancelled() {
			continue
		}
		b.tryDispatch(req)
	}
	// If nothing in flight can help and the last dial failed, fail the queue.
	if lastDialErr != nil && len(b.conns) == 0 && b.dialing == 0 {
		failQueue := b.queue
		b.queue = nil
		for _, req := range failQueue {
			select {
			case req.done <- lastDialErr:
			default:
			}
		}
	}
}

// handleConnLost is invoked by the scheduler when a pipeConn has died.
//
// Critical: between the conn's writer/reader exiting and the scheduler
// seeing connLost, the scheduler may have dispatched more requests to
// cn.reqCh. We must drain those here (nobody else will).
func (b *backend) handleConnLost(cn *pipeConn) {
	b.conns.remove(cn)

	// Drain any stragglers in reqCh that we dispatched after the conn's
	// writer exited. Once we remove cn from b.conns above, the scheduler
	// won't dispatch any new reqs to it, so this drain is bounded.
	for {
		select {
		case req := <-cn.reqCh:
			cn.recordReqChAtDeath(req)
		default:
			goto drained
		}
	}
drained:

	retryable := cn.harvest()
	for _, req := range retryable {
		if req.isCancelled() {
			continue
		}
		if req.idempotent && atomic.AddInt32(&req.attempts, 1) <= maxAttempts {
			b.queue = append(b.queue, req)
		} else {
			select {
			case req.done <- cn.closeErr:
			default:
			}
		}
	}
	b.drainQueueLocked(nil)
}

// doReap closes surplus idle conns, keeping at most MaxIdleConns warm.
// Called from the scheduler goroutine, so it has exclusive access to
// b.conns. A conn counts as "idle" when its atomic inFlight is 0. Busy
// conns (mid-op) are never closed; they'll be eligible at the next reap.
func (b *backend) doReap() {
	keep := b.c.maxIdleConns()
	// Count idle conns first so we only close surplus.
	idleCount := 0
	for cn := range b.conns {
		if atomic.LoadInt32(&cn.inFlight) == 0 {
			idleCount++
		}
	}
	surplus := idleCount - keep
	if surplus <= 0 {
		return
	}
	for cn := range b.conns {
		if surplus <= 0 {
			break
		}
		if atomic.LoadInt32(&cn.inFlight) == 0 {
			// The error value here is immaterial; the conn is being discarded.
			cn.close(errBackendClosed)
			surplus--
		}
	}
}

// shutdown is called when quit is signaled. It closes each conn and fails
// any queued reqs. Late dialDone/connLost sends won't block because those
// channels are buffered and any senders fall through to `<-b.quit` selects.
func (b *backend) shutdown() {
	for cn := range b.conns {
		cn.close(errBackendClosed)
	}
	for _, req := range b.queue {
		select {
		case req.done <- errBackendClosed:
		default:
		}
	}
	b.queue = nil
}

// pipeConn is a single pipelined connection to a server.
type pipeConn struct {
	be *backend
	nc net.Conn
	br *bufio.Reader
	bw *bufio.Writer

	reqCh   chan *pipeReq // scheduler → writer
	pending chan *pipeReq // writer → reader FIFO

	inFlight int32 // atomic; reqs in reqCh + pending + being served

	closeOnce sync.Once
	closeErr  error
	closeCh   chan struct{}

	// harvest state (protected by harvestMu)
	harvestMu      sync.Mutex
	harvestSent    bool
	writeFailures  []*pipeReq // failed before hitting the wire
	pendingAtDeath []*pipeReq // in pending at time of close
	reqChAtDeath   []*pipeReq // still in reqCh at time of close
}

func newPipeConn(be *backend, nc net.Conn) *pipeConn {
	return &pipeConn{
		be:      be,
		nc:      nc,
		br:      bufio.NewReader(nc),
		bw:      bufio.NewWriter(nc),
		reqCh:   make(chan *pipeReq, 256),
		pending: make(chan *pipeReq, 256),
		closeCh: make(chan struct{}),
	}
}

func (cn *pipeConn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.be.c.netTimeout()))
}

// close marks the conn dead and closes the underlying net.Conn. The first
// caller wins; subsequent calls are no-ops. It is safe to call from any
// goroutine.
func (cn *pipeConn) close(err error) {
	cn.closeOnce.Do(func() {
		cn.closeErr = err
		cn.nc.Close()
		close(cn.closeCh)
	})
}

// writer serializes writes onto the conn. Reads reqs from reqCh; on success,
// pushes them onto pending for the reader to decode. On error, records the
// failed req and stops. Does NOT drain reqCh on exit; the scheduler does
// that in handleConnLost (it's the only one that knows no more reqs will
// be dispatched to this conn).
func (cn *pipeConn) writer() {
	for {
		select {
		case req := <-cn.reqCh:
			if req.isCancelled() {
				// Caller already moved on; nothing hit the wire yet, so
				// no response framing to worry about. Just release the
				// slot and let the scheduler dispatch more.
				atomic.AddInt32(&cn.inFlight, -1)
				cn.signalIdle()
				continue
			}
			cn.extendDeadline()
			if err := req.write(cn.bw); err != nil {
				cn.recordWriteFailure(req)
				cn.close(err)
				close(cn.pending)
				return
			}
			if err := cn.bw.Flush(); err != nil {
				// Request may have partially reached the wire. Treat it as
				// "in flight" so retry honors idempotency.
				cn.pending <- req
				cn.close(err)
				close(cn.pending)
				return
			}
			cn.pending <- req
		case <-cn.closeCh:
			close(cn.pending)
			return
		}
	}
}

// reader reads responses in FIFO order from pending. On a non-resumable error,
// closes the conn; pending reqs in the channel at that moment are harvested.
//
// Cancelled reqs (whose caller's context expired after the request bytes
// hit the wire but before the response arrived) still have their response
// consumed here. Skipping the read would leave those bytes on the wire and
// misalign the parse of the NEXT req in the pipeline, corrupting it. The
// discarded result is dropped via non-blocking send to req.done.
func (cn *pipeConn) reader() {
	for req := range cn.pending {
		cn.extendDeadline()
		err := req.read(cn.br)
		atomic.AddInt32(&cn.inFlight, -1)

		if err != nil && !resumableError(err) {
			// Framing is suspect; close conn and harvest the rest.
			select {
			case req.done <- err:
			default:
			}
			cn.close(err)
			for remainder := range cn.pending {
				cn.recordPendingAtDeath(remainder)
			}
			break
		}
		// Non-blocking send: if the caller already moved on (ctx expired),
		// the buffer may be empty and we fill it (and it gets GC'd), or the
		// caller is still waiting and receives err.
		select {
		case req.done <- err:
		default:
		}
		cn.signalIdle()
	}
	select {
	case cn.be.connLost <- cn:
	case <-cn.be.quit:
	}
}

// signalIdle wakes the scheduler to consider dispatching queued reqs to a
// newly-idle conn. Non-blocking: the connIdle chan has capacity 1 and the
// scheduler's single drain pass dispatches across all idle conns, so dropped
// signals are harmless.
func (cn *pipeConn) signalIdle() {
	select {
	case cn.be.connIdle <- struct{}{}:
	default:
	}
}

func (cn *pipeConn) recordWriteFailure(req *pipeReq) {
	cn.harvestMu.Lock()
	cn.writeFailures = append(cn.writeFailures, req)
	cn.harvestMu.Unlock()
}

func (cn *pipeConn) recordPendingAtDeath(req *pipeReq) {
	cn.harvestMu.Lock()
	cn.pendingAtDeath = append(cn.pendingAtDeath, req)
	cn.harvestMu.Unlock()
}

func (cn *pipeConn) recordReqChAtDeath(req *pipeReq) {
	cn.harvestMu.Lock()
	cn.reqChAtDeath = append(cn.reqChAtDeath, req)
	cn.harvestMu.Unlock()
}

// harvest returns all reqs that need scheduler disposition. writeFailures
// never reached the wire (always safe to retry). pendingAtDeath were written
// and may or may not have been server-processed (idempotent-only retry per
// scheduler policy). reqChAtDeath were dispatched by the scheduler but never
// pulled by the writer: safe (never sent). Scheduler uses req.idempotent to
// decide per-req which to retry.
func (cn *pipeConn) harvest() []*pipeReq {
	cn.harvestMu.Lock()
	defer cn.harvestMu.Unlock()
	if cn.harvestSent {
		return nil
	}
	cn.harvestSent = true
	n := len(cn.writeFailures) + len(cn.pendingAtDeath) + len(cn.reqChAtDeath)
	out := make([]*pipeReq, 0, n)
	out = append(out, cn.writeFailures...)
	out = append(out, cn.pendingAtDeath...)
	out = append(out, cn.reqChAtDeath...)
	return out
}
