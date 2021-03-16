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

func BenchmarkPickServerWithBreaker(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_SingleWithBreaker(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss serversWithBreaker
	requireNoError(b, ss.resolveServers(servers...))
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}

// Note: this test is technically non-deterministic as the retry scheduling is async,
// but failures should be exceedingly rare due to the length of time until the first timeout.
//
// If a fix is required: probably simplest to change the startingWait to a var and modify in this test
func TestBehaviourSynchronously(t *testing.T) {
	create := func(t *testing.T) (ServerSelector, net.Addr, net.Addr) {
		ss, err := NewSelectorWithBreaker([]string{"127.0.0.1:4200", "127.255.0.1:4200"})
		requireNoError(t, err)
		wb := ss.(*serversWithBreaker)
		addrA := wb.addrs[0]
		addrB := wb.addrs[1]
		if addrA == addrB {
			t.Fatalf("for test to be valid addresses must differ")
		}
		return ss, addrA, addrB
	}

	t.Run("after a net error, an address should be not be pickable until retried", func(t *testing.T) {
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

	t.Run("after a net error, an address should be removed", func(t *testing.T) {
		ss, addrA, addrB := create(t)
		// when all address have errors, we return an error that signals no servers are available (circuit breaker)
		ss.OnResult(addrA, &net.OpError{})
		ss.OnResult(addrB, &net.OpError{})

		_, err := ss.PickServer("hi")
		assertEqualError(t, err, "memcache: no servers configured or available")
	})
}

func TestRetriesAfterError(t *testing.T) {
	ss := createWithServers(t, []string{"127.0.0.1:1234"})

	addrA := ss.addrs[0]
	ss.OnResult(addrA, &net.OpError{})
	_, err := ss.PickServer("hi")
	assertEqualError(t, err, "memcache: no servers configured or available")

	time.Sleep(startingWait * 2)
	_, err = ss.PickServer("hi")
	// should have become available again
	requireNoError(t, err)
}

func TestRecoversOnSuccessfulCommunication(t *testing.T) {
	ss := createWithServers(t, []string{"127.0.0.1:1234"})
	addrA := ss.addrs[0]

	ss.OnResult(addrA, &net.OpError{})
	_, err := ss.PickServer("hi")
	assertEqualError(t, err, "memcache: no servers configured or available")

	ss.OnResult(addrA, nil)
	_, err = ss.PickServer("hi")
	// should have become available again
	requireNoError(t, err)
}

func TestOneServer(t *testing.T) {
	functionalTest(t, []string{"42111"})
}

func TestTwoServers(t *testing.T) {
	functionalTest(t, []string{"42111", "42222"})
}

// This is a functional test that spins up and repeatedly takes down a memcached node.
func functionalTest(t *testing.T, ports []string) {
	srvs := []string{}
	for _, port := range ports {
		srvs = append(srvs, fmt.Sprintf("localhost:%s", port))
	}
	cli := New(srvs...)

	errCounts := map[string]int{}
	done := make(chan struct{})
	wg := sync.WaitGroup{}
	networkErrorCategory := "[network error]"
	internalTimeoutError := "[memcachepl timeout]"

	server := func(port string) {
		time.Sleep(time.Millisecond * time.Duration(rand.Int31n(25)))
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			cmd := exec.CommandContext(ctx, "memcached", "-p", port)
			err := cmd.Run()
			assertEqualError(t, err, "signal: killed")
			time.Sleep(time.Millisecond*500 + time.Duration(rand.Int31n(50)))
		}
		wg.Done()
	}

	wg.Add(len(srvs))
	for _, port := range ports {
		go server(port)
	}

	go (func() {
		wg.Wait()
		close(done)
	})()

	mx := sync.Mutex{}
	writer := func() {
		for {
			select {
			case <-done:
				break
			default:
				err := cli.Set(&Item{
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
				errCounts[errString]++
				mx.Unlock()
			}
			time.Sleep(time.Duration(rand.Int31n(5)) * time.Millisecond)
		}
	}

	go writer()
	go writer()
	go writer()
	go writer()
	go writer()
	go writer()

	wg.Wait()

	mx.Lock()
	defer mx.Unlock()

	unexpectedErrors := map[string]struct{}{}
	total := 0
	for errVal, count := range errCounts {
		total += count
		switch errVal {

		// filter out expected errors
		case "<nil>":
		case "EOF":
		case internalTimeoutError:
		case networkErrorCategory:
		case ErrNoServers.Error():

		default:
			unexpectedErrors[errVal] = struct{}{}
		}
	}

	// we're up for approximately 2.5 seconds out of every three, so we should have a lot of successes too
	assertGreaterOrEqual(t, errCounts["<nil>"], total/10, "expected many successes")

	for s := range unexpectedErrors {
		t.Errorf("unexpected errors during test - either indicates a bug or something that needs categorisation: %s", s)
	}

	// test that we have greatly more breaker errors than network errors, reducing the reconnection by a large factor
	networkFails := errCounts[networkErrorCategory] + errCounts[internalTimeoutError] +
		errCounts["EOF"]
	assertGreaterOrEqual(t, errCounts[ErrNoServers.Error()], networkFails*10, "didn't reduce reconnection load as expected")
}

func fuzzer(withKey func(s string)) {
	rand.Seed(42)
	for i := 0; i < 100; i++ {
		withKey(fmt.Sprintf("key%d", rand.Int63()))
	}
}

func createWithServers(t testing.TB, srvs []string) *serversWithBreaker {
	ss, err := NewSelectorWithBreaker(srvs)
	requireNoError(t, err)
	return ss.(*serversWithBreaker)
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

func assertGreaterOrEqual(t testing.TB, expected, val int, msg string) {
	t.Helper()
	if val < expected {
		t.Errorf("%s: expected >= %d, got %d", msg, expected, val)
	}
}
