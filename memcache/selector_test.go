/*
Copyright 2014 Google Inc.

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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestAllocations(t *testing.T) {
	t.Run("no allocations with two servers", func(t *testing.T) {
		benchPickServer(t, "127.0.0.1:1234", "127.0.0.1:1235")
	})
	t.Run("no allocations with one server", func(t *testing.T) {
		benchPickServer(t, "127.0.0.1:1234")
	})
}


func benchPickServer(t *testing.T, servers ...string) {
	br := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		var ss ServerList
		requireNoError(b, ss.SetServers(servers...))
		for i := 0; i < b.N; i++ {
			if _, err := ss.PickServer("some key"); err != nil {
				b.Fatal(err)
			}
		}
	})
	if br.AllocsPerOp() > 0 {
		t.Errorf("expected no allocations, got %d", br.AllocsPerOp())
	}
}

// Note: this test is technically non-deterministic as the retry scheduling is async,
// but failures should be exceedingly rare due to the length of time until the first timeout.
//
// If a fix is required: probably simplest to change the startingWait to a var and modify in this test
func TestBehaviourSynchronously(t *testing.T) {
	create := func(t *testing.T) (ServerSelector, net.Addr, net.Addr) {
		ss := createWithServers(t, []string{"127.0.0.1:4200", "127.255.0.1:4200"})
		addrA := ss.addrs[0]
		addrB := ss.addrs[1]
		if addrA == addrB {
			t.Fatalf("for test to be valid addresses must differ")
		}
		return ss, addrA, addrB
	}

	t.Run("after a net error, an address should be not be pickable", func(t *testing.T) {
		ss, addrA, _ := create(t)

		ss.OnResult(addrA, &net.OpError{})
		fuzzer(func(k string) {
			addr, err := ss.PickServer(k)
			requireNoError(t, err)
			if addrA == addr {
				t.Fatalf("address still available")
			}
		})
	})

	t.Run("after a successful retry, an address should be available again", func(t *testing.T) {
		ss, addrA, _ := create(t)

		ss.OnResult(addrA, &net.OpError{})
		ss.OnResult(addrA, nil)

		gotAddr := 0
		fuzzer(func(k string) {
			addr, err := ss.PickServer(k)
			requireNoError(t, err)
			if addr == addrA {
				gotAddr++
			}
		})
		if gotAddr == 0 {
			t.Fatalf("address not pickable again")
		}
	})

	t.Run("on error a retry is scheduled, and after the wait the addr is available again", func(t *testing.T) {
		ss, addrA, addrB := create(t)
		// when all address have errors, we return an error that signals no servers are available (circuit breaker)
		ss.OnResult(addrA, &net.OpError{})
		ss.OnResult(addrB, &net.OpError{})

		_, err := ss.PickServer("hi")
		assertEqualError(t, err, ErrNoServers.Error())

		// ensure that the retry goroutine that schedule didn't make the server available again before we expect
		time.Sleep(startingWait / 2)
		_, err = ss.PickServer("hi")
		assertEqualError(t, err, ErrNoServers.Error())

		// and that after the retry wait expires we do schedule a retry
		time.Sleep(startingWait * 2)
		a, err := ss.PickServer("hi")
		if err != nil {
			t.Fatalf("should have become eligable for a retry")
		}
		if a == nil {
			t.Fatalf("no addr returned for retry")
		}
	})
}

func TestRecoversOnSuccessfulCommunication(t *testing.T) {
	ss := createWithServers(t, []string{"127.0.0.1:1234"})
	addrA := ss.addrs[0]

	ss.OnResult(addrA, &net.OpError{})
	_, err := ss.PickServer("hi")
	assertEqualError(t, err, ErrNoServers.Error())

	ss.OnResult(addrA, nil)
	_, err = ss.PickServer("hi")
	// should have become available again
	requireNoError(t, err)
}

func TestInternalsTests(t *testing.T) {
	// these tests are fragile vs implementation changes, but worth it to give us confidence that the
	// methods work before we layer on concurrency
	t.Run("filterAvailable filters out addresses listed multiple times to support load-balancing", func(t *testing.T) {
		// single routine, so not using the locking that should be used on these methods
		ss := &ServerList{}
		srvA := "127.0.0.1:1234"
		srvB := "127.255.0.1:1234"
		ss.SetServers(srvA, srvA, srvB)
		// relies on SetServers resolving in order
		addrA := ss.addrs[0]
		ss.setState(addrA, waitState{
			retry: retryWait,
		})
		if len(ss.available) != 1 {
			t.Fatalf("should have filtered out unavailable servers")
		}
		if ss.available[0].String() != srvB {
			t.Fatalf("expected %q to be available, but found %q", srvA, ss.available[0].String())
		}
	})

	t.Run("filterAvailable doesn't filter available servers", func(t *testing.T) {
		// single routine, so not using the locking that should be used on these methods
		ss := &ServerList{}
		srvA := "127.0.0.1:1234"
		ss.SetServers(srvA, srvA)
		ss.filterAvailable()
		if len(ss.available) != 2 {
			t.Fatalf("unexpectedly filtered servers")
		}
	})

	t.Run("filterAvailable considers retryReady servers available", func(t *testing.T) {
		ss := &ServerList{}
		ss.SetServers("127.0.0.1:1234")
		ss.setState(ss.addrs[0], waitState{
			retry: retryReady,
		})
		if len(ss.available) != 1 {
			t.Fatalf("unexpectedly filtered servers")
		}
	})

	t.Run("filterAvailable does not consider retryRunning servers available", func(t *testing.T) {
		ss := &ServerList{}
		ss.SetServers("127.0.0.1:1234")
		ss.setState(ss.addrs[0], waitState{
			retry: retryRunning,
		})
		if len(ss.available) > 0 {
			t.Fatalf("should have filtered server")
		}
	})

	t.Run("servers are available after state is removed", func(t *testing.T) {
		ss := &ServerList{}
		ss.SetServers("127.0.0.1:1234")
		ss.setState(ss.addrs[0], waitState{
			retry: retryRunning,
		})
		ss.deleteState(ss.addrs[0])
		if len(ss.available) != 1 {
			t.Fatalf("server should be available")
		}
	})
}

func TestOneServer(t *testing.T) {
	functionalTest(t, []string{"42111"})
}

func TestTwoServers(t *testing.T) {
	functionalTest(t, []string{"42111", "42222"})
}

// This is a functional test that spins up and repeatedly takes down a memcached nodes
// while multiple writer routines attempt to Set keys
func functionalTest(t *testing.T, ports []string) {
	srvs := []string{}
	for _, port := range ports {
		srvs = append(srvs, fmt.Sprintf("localhost:%s", port))
	}
	client := New(srvs...)

	wg := sync.WaitGroup{}

	server := func(port string) {
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			cmd := exec.CommandContext(ctx, "memcached", "-p", port)
			err := cmd.Run()
			assertEqualError(t, err, "signal: killed")

			// no randomisation - keep approximately in lockstep to keep the amount of downtime high
			time.Sleep(time.Millisecond*500)
		}
		wg.Done()
	}

	done := make(chan struct{})
	mx := sync.Mutex{}
	resultCounts := map[string]int{}
	networkErrorCategory := "[network error]"
	internalTimeoutError := "[memcachepl timeout]"
	writer := func() {
		for {
			select {
			case <-done:
				break
			default:
				err := client.Set(&Item{
					Key:   "hi",
					Value: []byte("val"),
				})
				errString := fmt.Sprintf("%v", err)
				var netErr net.Error
				if errors.As(err, &netErr) {
					errString = networkErrorCategory
				}
				var cte *ConnectTimeoutError
				if errors.As(err, &cte) {
					errString = internalTimeoutError
				}
				mx.Lock()
				resultCounts[errString]++
				mx.Unlock()
			}
			time.Sleep(time.Duration(rand.Int31n(5)) * time.Millisecond)
		}
	}

	// start servers
	wg.Add(len(srvs))
	for _, port := range ports {
		go server(port)
	}

	// start writers
	go writer()
	go writer()
	go writer()
	go writer()
	go writer()
	go writer()

	// wait for test run to end
	wg.Wait()
	close(done)

	// avoid race-detection errors when we range over results
	mx.Lock()
	defer mx.Unlock()

	unexpectedErrors := map[string]struct{}{}
	total := 0
	for errVal, count := range resultCounts {
		total += count
		switch errVal {

		// filter out success, and expected errors
		case "<nil>":
		case "EOF":
		case internalTimeoutError:
		case networkErrorCategory:
		case ErrNoServers.Error():

		default:
			unexpectedErrors[errVal] = struct{}{}
		}
	}

	t.Log("aggregate result of Set calls", resultCounts)

	for s := range unexpectedErrors {
		t.Errorf("unexpected errors during test - either indicates a bug or something that needs categorisation: %s", s)
	}

	// These ratios are just rough observed values from the configuration above setting the amount
	// of time the memcache nodes are available. They're not tight boundaries as that would create test
	// flakiness. They're designed to ensure that the implementation is having the desired effect.

	// We're up for approximately 2.5 seconds out of every three, so we should have decent number of success too
	// Enforcing success ratio avoids allowing a bug that causes us to return ErrNoServers continually and thus passes the 'more breaker
	// errors than network errors' test below.
	expectedSuccessRatio := 10
	assertGreaterOrEqual(t, resultCounts["<nil>"], total/expectedSuccessRatio, "expected many successes")

	// We want greatly more breaker errors than network errors, reducing the reconnections attempts during unavailability by a large factor
	// Although this is controlled by the test configuration above, what we're demonstrating here is that we do manage to prevent a spike
	// in reconnections during the 5 up->down transitions
	breakerToNetworkErrorRatio := 80
	networkFails := resultCounts[networkErrorCategory] + resultCounts[internalTimeoutError] +
		resultCounts["EOF"]
	assertGreaterOrEqual(t, resultCounts[ErrNoServers.Error()], networkFails*breakerToNetworkErrorRatio, "didn't reduce reconnection load as expected")
}

func fuzzer(withKey func(s string)) {
	rand.Seed(42)
	for i := 0; i < 100; i++ {
		withKey(fmt.Sprintf("key%d", rand.Int63()))
	}
}

func createWithServers(t testing.TB, srvs []string) *ServerList {
	ss := &ServerList{}
	err := ss.SetServers(srvs...)
	requireNoError(t, err)
	return ss
}

func requireNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
}

func assertEqualError(t testing.TB, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Errorf("expected error")
	} else if err.Error() != msg {
		t.Errorf("expected error %q got %q", msg, err.Error())
	}
}

func assertGreaterOrEqual(t testing.TB, candidate, comparison int, msg string) {
	t.Helper()
	if candidate < comparison {
		t.Errorf("%s: expected >= %d, got %d", msg, comparison, candidate)
	}
}
