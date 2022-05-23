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

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const testServer = "localhost:11211"

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", testServer)
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}
	testWithClient(t, New(testServer))
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d.sock", os.Getpid())
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		t.Skipf("skipping test; couldn't find memcached")
		return
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	testWithClient(t, New(sock))
}

func mustSetF(t *testing.T, c *Client) func(*Item) {
	return func(it *Item) {
		if err := c.Set(it); err != nil {
			t.Fatalf("failed to Set %#v: %v", *it, err)
		}
	}
}

func testWithClient(t *testing.T, c *Client) {
	checkErr := func(err error, format string, args ...interface{}) {
		if err != nil {
			t.Fatalf(format, args...)
		}
	}
	mustSet := mustSetF(t, c)

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	err := c.Set(foo)
	checkErr(err, "first set(foo): %v", err)
	err = c.Set(foo)
	checkErr(err, "second set(foo): %v", err)

	// Get
	it, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
	}
	if it.Flags != 123 {
		t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
	}

	// Get and set a unicode key
	quxKey := "Hello_世界"
	qux := &Item{Key: quxKey, Value: []byte("hello world")}
	err = c.Set(qux)
	checkErr(err, "first set(Hello_世界): %v", err)
	it, err = c.Get(quxKey)
	checkErr(err, "get(Hello_世界): %v", err)
	if it.Key != quxKey {
		t.Errorf("get(Hello_世界) Key = %q, want Hello_世界", it.Key)
	}
	if string(it.Value) != "hello world" {
		t.Errorf("get(Hello_世界) Value = %q, want hello world", string(it.Value))
	}

	// Set malformed keys
	malFormed := &Item{Key: "foo bar", Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
	}
	malFormed = &Item{Key: "foo" + string(rune(0x7f)), Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	checkErr(err, "first add(foo): %v", err)
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
	}

	// Replace
	baz := &Item{Key: "baz", Value: []byte("bazvalue")}
	if err := c.Replace(baz); err != ErrNotStored {
		t.Fatalf("expected replace(baz) to return ErrNotStored, got %v", err)
	}
	err = c.Replace(bar)
	checkErr(err, "replaced(foo): %v", err)

	// GetMulti
	m, err := c.GetMulti([]string{"foo", "bar"})
	checkErr(err, "GetMulti: %v", err)
	if g, e := len(m), 2; g != e {
		t.Errorf("GetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m["foo"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'foo'")
	}
	if _, ok := m["bar"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'bar'")
	}
	if g, e := string(m["foo"].Value), "fooval"; g != e {
		t.Errorf("GetMulti: foo: got %q, want %q", g, e)
	}
	if g, e := string(m["bar"].Value), "barval"; g != e {
		t.Errorf("GetMulti: bar: got %q, want %q", g, e)
	}

	// Delete
	err = c.Delete("foo")
	checkErr(err, "Delete: %v", err)
	it, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
	}

	// Incr/Decr
	mustSet(&Item{Key: "num", Value: []byte("42")})
	n, err := c.Increment("num", 8)
	checkErr(err, "Increment num + 8: %v", err)
	if n != 50 {
		t.Fatalf("Increment num + 8: want=50, got=%d", n)
	}
	n, err = c.Decrement("num", 49)
	checkErr(err, "Decrement: %v", err)
	if n != 1 {
		t.Fatalf("Decrement 49: want=1, got=%d", n)
	}
	err = c.Delete("num")
	checkErr(err, "delete num: %v", err)
	n, err = c.Increment("num", 1)
	if err != ErrCacheMiss {
		t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
	}
	mustSet(&Item{Key: "num", Value: []byte("not-numeric")})
	n, err = c.Increment("num", 1)
	if err == nil || !strings.Contains(err.Error(), "client error") {
		t.Fatalf("increment non-number: want client error, got %v", err)
	}
	testTouchWithClient(t, c)

	// Test Delete All
	err = c.DeleteAll()
	checkErr(err, "DeleteAll: %v", err)
	it, err = c.Get("bar")
	if err != ErrCacheMiss {
		t.Errorf("post-DeleteAll want ErrCacheMiss, got %v", err)
	}

	// Test Ping
	err = c.Ping()
	checkErr(err, "error ping: %s", err)

	// test meta commands
	testMetaGetCommandsWithClient(t, c, checkErr)
	testMetaSetCommandsWithClient(t, c, checkErr)
	testMetaDeleteCommandsWithClient(t, c, checkErr)
	testMetaArithmeticCommandsWithClient(t, c, checkErr)
	testMetaDebugCommandsWithClient(t, c, checkErr)
}

func testTouchWithClient(t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcache Touch with testing in Short mode")
		return
	}

	mustSet := mustSetF(t, c)

	const secondsToExpiry = int32(2)

	// We will set foo and bar to expire in 2 seconds, then we'll keep touching
	// foo every second
	// After 3 seconds, we expect foo to be available, and bar to be expired
	foo := &Item{Key: "foo", Value: []byte("fooval"), Expiration: secondsToExpiry}
	bar := &Item{Key: "bar", Value: []byte("barval"), Expiration: secondsToExpiry}

	setTime := time.Now()
	mustSet(foo)
	mustSet(bar)

	for s := 0; s < 3; s++ {
		time.Sleep(time.Duration(1 * time.Second))
		err := c.Touch(foo.Key, secondsToExpiry)
		if nil != err {
			t.Errorf("error touching foo: %v", err.Error())
		}
	}

	_, err := c.Get("foo")
	if err != nil {
		if err == ErrCacheMiss {
			t.Fatalf("touching failed to keep item foo alive")
		} else {
			t.Fatalf("unexpected error retrieving foo after touching: %v", err.Error())
		}
	}

	_, err = c.Get("bar")
	if nil == err {
		t.Fatalf("item bar did not expire within %v seconds", time.Now().Sub(setTime).Seconds())
	} else {
		if err != ErrCacheMiss {
			t.Fatalf("unexpected error retrieving bar: %v", err.Error())
		}
	}
}

func testMetaArithmeticCommandsWithClient(t *testing.T, c *Client,
	checkErr func(err error, format string, args ...interface{})) {

	c.DeleteAll()
	defer c.DeleteAll()

	var response *MetaResponseMetadata

	//meta arithmetic test when the key is not found
	_, err := c.MetaArithmetic("k1", &MetaArithmeticFlags{})
	if err != ErrCacheMiss {
		t.Errorf("metaArithmetic(k1) expected error ErrCacheMiss instead of %v", err)
	}

	//meta arithmetic on a non numeric value
	stringItem := &Item{Key: "k0", Value: []byte("fooval"), Flags: 123}
	err = c.Set(stringItem)
	checkErr(err, "first set(stringItem): %v", err)
	response, err = c.MetaArithmetic("k0", &MetaArithmeticFlags{})
	if err == nil {
		t.Errorf("metaArithmetic(k0) error should not be nil")
	}

	//meta arithmetic with non-existing key and AutoCreateItemOnMissTTLToken
	var itemTTL int32 = 300
	response, err = c.MetaArithmetic("k1", &MetaArithmeticFlags{AutoCreateItemOnMissTTLToken: &itemTTL})
	checkErr(err, "metaArithmetic(k1): %v", err)
	if response == nil {
		t.Errorf("metaArithmetic(k1) response should not be nil")
	}

	//meta arithmetic with non-existing key , create and return Cas , ttl and value.
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{ReturnItemValueInResponse: true,
		AutoCreateItemOnMissTTLToken: &itemTTL, ReturnCasTokenInResponse: true, ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "metaArithmetic(k2) return value: %v", err)
	if response == nil || string(response.ReturnItemValue) != "0" {
		t.Errorf("metaArithmetic(k2) Actual Value=%q, Expected Value=0", string(response.ReturnItemValue))
	}
	if response.TTLRemainingInSeconds == nil || *response.TTLRemainingInSeconds == 0 {
		t.Errorf("metaArithmetic(k2) TTL remaining should not be nil or zero")
	}
	if response.CasId == nil {
		t.Errorf("metaArithmetic(k2) CasId should not be nil")
	}
	casIdk2 := *response.CasId

	//meta arithmetic with existing key and default increment mode with delta 1
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{CompareCasTokenToUpdateValue: &casIdk2,
		ReturnItemValueInResponse: true, ReturnCasTokenInResponse: true})
	checkErr(err, "metaArithmetic(k2) with cas token return value: %v", err)
	if response == nil || string(response.ReturnItemValue) != "1" {
		t.Errorf("metaArithmetic(k2) with cas token Actual Value=%q, Expected Value=1", string(response.ReturnItemValue))
	}

	//meta arithmetic with invalid cas token
	invalidCasToken := (*response.CasId) + 1
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{CompareCasTokenToUpdateValue: &invalidCasToken,
		ReturnItemValueInResponse: true})
	if err != ErrCASConflict {
		t.Errorf("metaArithmetic(k2) expected error ErrCASConflict instead of %v", err)
	}

	//meta arithmetic with increment mode and delta
	incrModeToken := MetaArithmeticIncrement
	var deltaToken uint64 = 5
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{ArithmeticModeToken: &incrModeToken,
		DeltaToken: &deltaToken, ReturnItemValueInResponse: true})
	checkErr(err, "metaArithmetic(k2) with increment and delta value: %v", err)
	if response == nil || string(response.ReturnItemValue) != "6" {
		t.Errorf("metaArithmetic(k2)  with increment and delta Actual Value=%q, Expected Value=6", string(response.ReturnItemValue))
	}

	//meta arithmetic with decrement mode and delta
	decrModeToken := MetaArithmeticDecrement
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{ArithmeticModeToken: &decrModeToken,
		DeltaToken: &deltaToken, ReturnItemValueInResponse: true})
	checkErr(err, "metaArithmetic(k2) with decrement and delta value: %v", err)
	if response == nil || string(response.ReturnItemValue) != "1" {
		t.Errorf("metaArithmetic(k2)  with decrement and delta Actual Value=%q, Expected Value=1", string(response.ReturnItemValue))
	}

	//meta arithmetic mode update TTL token and return ttl
	var updateTTLToken int32 = 600
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{UpdateTTLToken: &updateTTLToken,
		ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "metaArithmetic(k2) with update ttl and fetch ttl: %v", err)
	if response == nil || response.TTLRemainingInSeconds == nil || *response.TTLRemainingInSeconds == 0 {
		t.Errorf("metaArithmetic(k2)  with update ttl and fetch ttl, response should not be nil and " +
			"TTLRemainingInSeconds should not be nil or zero")
	}

	//meta arithmetic with no reply semantics. Error is always thrown in this case
	response, err = c.MetaArithmetic("k2", &MetaArithmeticFlags{UseNoReplySemanticsForResponse: true})
	if err == nil {
		t.Errorf("metaArithmetic(k2)  with no reply semantics err should not be nil and its expected")
	}

	//meta arithmetic with initial value set and base encoded key
	//azM=base64Encode(k3)
	var initialValue uint64 = 5
	response, err = c.MetaArithmetic("azM=", &MetaArithmeticFlags{AutoCreateItemOnMissTTLToken: &itemTTL,
		AutoCreateInitialValueOnMissToken: &initialValue, ReturnItemValueInResponse: true, IsKeyBase64: true})
	checkErr(err, "metaArithmetic(k3) with initial value set and base encoded key: %v", err)
	if response == nil || string(response.ReturnItemValue) != "5" {
		t.Errorf("metaArithmetic(k3) with initial value set and base encoded key"+
			" Actual Value=%q, Expected Value=5", string(response.ReturnItemValue))
	}
	//fetch the item with decoded key. This ensures that IsKeyBase64 option is working correct
	var item *Item
	item, err = c.Get("k3")
	checkErr(err, "get(k3): %v", err)
	if item == nil || item.Key != "k3" {
		t.Errorf("get(k3) item should not be nil and the key should be k3.Actual key:%q ", item.Key)
	}

	//meta arithmetic with malformed key
	response, err = c.MetaArithmetic("malformed key", &MetaArithmeticFlags{})
	if err != ErrMalformedKey {
		t.Errorf("metaArithmetic(malformed key) expected error ErrMalformedKey instead of %v", err)
	}

	//meta arithmetic with AutoCreateInitialValueOnMissToken and no TTL. Cache miss is expected here.
	response, err = c.MetaArithmetic("k4", &MetaArithmeticFlags{AutoCreateInitialValueOnMissToken: &initialValue})
	if err != ErrCacheMiss {
		t.Errorf("metaArithmetic(k4) expected ErrCacheMiss instead of %v", err)
	}

}

func testMetaGetCommandsWithClient(t *testing.T, c *Client,
	checkErr func(err error, format string, args ...interface{})) {
	c.DeleteAll()
	defer c.DeleteAll()

	//preparing some test data for test cases
	key := &Item{Key: "key", Value: []byte("value")}
	err := c.Set(key)
	checkErr(err, "first set(key): %v", err)

	key2 := &Item{Key: "key2", Value: []byte("value\r\n"), Flags: 345}
	err = c.Set(key2)
	checkErr(err, "second set(key): %v", err)

	key3 := &Item{Key: "key3", Value: []byte("value 3")}
	err = c.Set(key3)
	checkErr(err, "third set(key3): %v", err)

	//simple meta get with nil flags. Should expect error
	respMetadata, err := c.MetaGet("key", nil)
	if err == nil {
		t.Errorf("metaGet(key) error expected as no options are enabled")
	}

	respMetadata, err = c.MetaGet("key", &MetaGetFlags{ReturnItemValueInResponse: true})
	checkErr(err, "second metaGet(key): %v", err)
	if string(respMetadata.ReturnItemValue) != "value" {
		t.Errorf("metaGet(key) Actual Value=%q, Expected Value=value", string(respMetadata.ReturnItemValue))
	}

	//meta get with base64 key , returnItemHitInResponse, LastAccessedTime, ItemSize and flags
	respMetadata, err = c.MetaGet("a2V5Mg==", &MetaGetFlags{IsKeyBase64: true,
		ReturnItemHitInResponse: true, ReturnLastAccessedTimeSecondsInResponse: true,
		ReturnItemSizeBytesInResponse: true, ReturnClientFlagsInResponse: true,
		ReturnItemValueInResponse: true,
		ReturnKeyInResponse:       true})
	checkErr(err, "third metaGet(key2): %v", err)
	if string(respMetadata.ReturnItemValue) != "value\r\n" {
		t.Errorf("metaGet(key2) Value=%q, Expected Value=value", string(respMetadata.ReturnItemValue))
	}
	if respMetadata.IsItemHitBefore == nil || *respMetadata.IsItemHitBefore == true {
		t.Errorf("metaGet(key2) IsItemHitBefore should not be nil but should be false")
	}
	if respMetadata.TimeInSecondsSinceLastAccessed == nil || *respMetadata.TimeInSecondsSinceLastAccessed != 0 {
		t.Errorf("metaGet(key2) TimeInSecondsSinceLastAccessed should not be nil but should be 0")
	}
	if respMetadata.ItemSizeInBytes == nil || *respMetadata.ItemSizeInBytes != 7 {
		t.Errorf("metaGet(key2) ItemSizeInBytes should not be nil but should be 7")
	}
	if respMetadata.ClientFlag == nil || *respMetadata.ClientFlag != 345 {
		t.Errorf("metaGet(key2) ClientFlag should not be nil but should be 345")
	}
	if respMetadata.ItemKey == nil || *respMetadata.ItemKey != "key2" {
		t.Errorf("metaGet(key2) ItemKey should not be nil but should be key2")
	}

	//sleep so that we can test ReturnLastAccessedTimeInSeconds
	time.Sleep(2 * time.Second)

	respMetadata, err = c.MetaGet("key", &MetaGetFlags{
		ReturnItemHitInResponse: true, ReturnLastAccessedTimeSecondsInResponse: true})
	checkErr(err, "metaGet(key3): %v", err)
	if respMetadata.IsItemHitBefore == nil || *respMetadata.IsItemHitBefore == false {
		t.Errorf("metaGet(key3) IsItemHitBefore should not be nil but should be true")
	}
	if respMetadata.TimeInSecondsSinceLastAccessed == nil || *respMetadata.TimeInSecondsSinceLastAccessed == 0 {
		t.Errorf("metaGet(key2) TimeInSecondsSinceLastAccessed should not be nil but should be non zero")
	}
	if respMetadata.ReturnItemValue != nil {
		t.Errorf("metaGet(key2) ReturnItemValue should be nil")
	}

	//meta get cache miss
	respMetadata, err = c.MetaGet("key53", &MetaGetFlags{ReturnItemValueInResponse: true})
	if err != ErrCacheMiss {
		t.Errorf("metaGet(key53) expected error ErrCacheMiss instead of %v", err)
	}

	//meta get with malformed key
	respMetadata, err = c.MetaGet("key val", nil)
	if err != ErrMalformedKey {
		t.Errorf("metaGet(key val) should return ErrMalformedKey instead of %v", err)
	}

	//meta get with cas response flag , ttl response flag , key response flag , Opaque token
	opaqueToken := "Opaque"
	respMetadata, err = c.MetaGet("key", &MetaGetFlags{ReturnCasTokenInResponse: true,
		ReturnTTLRemainingSecondsInResponse: true, ReturnKeyInResponse: true,
		OpaqueToken: &opaqueToken})
	checkErr(err, "cas,ttl metaGet(key): %v", err)
	if respMetadata.CasId == nil {
		t.Errorf("metaGet(key) casid should not be nil")
	}
	if respMetadata.TTLRemainingInSeconds == nil || *respMetadata.TTLRemainingInSeconds != -1 {
		t.Errorf("metaGet(key) TTLRemainingInSeconds should not be nil or should be -1 ")
	}
	if respMetadata.ItemKey == nil || *respMetadata.ItemKey != "key" {
		t.Errorf("metaGet(key) ItemKey should not be nil. Should be key")
	}
	if respMetadata.OpaqueToken == nil || *respMetadata.OpaqueToken != "Opaque" {
		t.Errorf("metaGet(key)  OpaqueToken should not be nil. Should be Opaque")
	}

	//meta get update ttl and fetch ttl
	var updateTTlToken int32 = 5000
	respMetadata, err = c.MetaGet("key", &MetaGetFlags{UpdateTTLToken: &updateTTlToken,
		ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "ttl,update ttl metaGet(key): %v", err)
	if respMetadata.TTLRemainingInSeconds == nil || *respMetadata.TTLRemainingInSeconds != 5000 {
		t.Errorf("metaGet(key) TTLRemainingInSeconds should not be nil. should be 5000")
	}

	//test PreventBumpInLRU flag
	key4 := &Item{Key: "key4", Value: []byte("value")}
	err = c.Set(key4)
	checkErr(err, "set(key4): %v", err)
	respMetadata, err = c.MetaGet("key4", &MetaGetFlags{PreventBumpInLRU: true})
	checkErr(err, "metaGet(key4): %v", err)

	respMetadata, err = c.MetaGet("key4", &MetaGetFlags{ReturnLastAccessedTimeSecondsInResponse: true,
		ReturnItemHitInResponse: true})
	checkErr(err, "metaGet(key4): %v", err)
	if respMetadata.IsItemHitBefore == nil || *respMetadata.IsItemHitBefore == true {
		t.Errorf("metaGet(key4) IsItemHitBefore should not be nil but should be false")
	}

	//testVivify TTL token
	var vivifyTTLToken int32 = 300
	respMetadata, err = c.MetaGet("key5", &MetaGetFlags{VivifyTTLToken: &vivifyTTLToken})
	checkErr(err, "metaGet(key5): %v", err)
	if !respMetadata.IsReCacheWon {
		t.Errorf("metaGet(key5) IsReCacheWon should be true instead of false")
	}
	if string(respMetadata.ReturnItemValue) != "" {
		t.Errorf("metaGet(key5) value should be empty")
	}

	respMetadata, err = c.MetaGet("key5", &MetaGetFlags{VivifyTTLToken: &vivifyTTLToken})
	if respMetadata.IsReCacheWon || !respMetadata.IsReCacheWonFlagAlreadySent {
		t.Errorf("metaGet(key5) IsReCacheWonFlagAlreadySent should be true. IsReCacheWon should be false")
	}

	//testRecacheTTLToken
	key6 := &Item{Key: "key6", Value: []byte("value"), Expiration: 300}
	err = c.Set(key6)
	checkErr(err, "set(key6): %v", err)
	time.Sleep(2 * time.Second)

	var reCacheTTLToken int32 = 300
	respMetadata, err = c.MetaGet("key6", &MetaGetFlags{ReCacheTTLToken: &reCacheTTLToken,
		ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "metaGet(key6): %v", err)
	if !respMetadata.IsReCacheWon {
		t.Errorf("metaGet(key6) IsReCacheWon should be true")
	}
	respMetadata, err = c.MetaGet("key6", &MetaGetFlags{ReturnItemValueInResponse: true})
	checkErr(err, "metaGet(key6): %v", err)
	if respMetadata.IsReCacheWon || !respMetadata.IsReCacheWonFlagAlreadySent {
		t.Errorf("metaGet(key6) IsReCacheWon should be false and IsReCacheWonFlagAlreadySent should be true")
	}
}

func testMetaSetCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	key := "bah"
	value := []byte("bahval")
	opaqueToken := "A123"
	metaFoo := &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{ReturnKeyInResponse: true, ReturnCasTokenInResponse: true, OpaqueToken: &opaqueToken}}
	response, err := c.MetaSet(metaFoo)
	if response.ItemKey == nil || *response.ItemKey != key {
		t.Errorf("meta set(%s) key should not be nil and should be %s", key, key)
	}
	if response.OpaqueToken == nil || *response.OpaqueToken != opaqueToken {
		t.Errorf("meta set(%s) Opaque token should not be nil and should be %s", key, opaqueToken)
	}
	casToken := response.CasId
	if casToken == nil {
		t.Errorf("meta set(%s) error, no CAS token returned", key)
	}
	checkErr(err, "normal meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the same cas token as what was last set
	value = []byte("new_bah_val")
	var newTTL int32 = 900000
	var clientFlagToken uint32 = 90
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{CompareCasTokenToUpdateValue: casToken, ClientFlagToken: &clientFlagToken, UpdateTTLToken: &newTTL}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "Same CAS token meta set(%s): %v", key, err)
	it, err := c.Get(key)
	if it.Flags != clientFlagToken {
		t.Errorf("Same CAS token meta set(%s) expected client flag %d but got %d", key, clientFlagToken, it.Flags)
	}
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using a different cas token
	value = []byte("byte_val_invalid")
	var newCasToken uint64 = 123456789
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{CompareCasTokenToUpdateValue: &newCasToken}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrCASConflict {
		t.Errorf("Different CAS token meta set(%s) expected an CAS conflict error but got %e", key, err)
	}

	// set with no reply semantics turned on
	// note that the documentation says that this flag will always return an error (even if the command runs successfully)
	value = []byte("with_base64_key")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{UseNoReplySemanticsForResponse: true}}
	_, err = c.MetaSet(metaFoo)
	// the error raised is an internal error, so we can't change for explicit type
	if err == nil {
		t.Errorf("no reply meta set(%s) expected an error to be returned but got none", key)
	}

	// set using the append mode with existing key
	valueToAppend := []byte("append_value_to_existing")
	value = append(value, valueToAppend...)
	mode := Append
	metaFoo = &MetaSetItem{Key: key, Value: valueToAppend, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful append meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the prepend mode
	valueToPrepend := []byte("prepend_value_to_existing")
	value = append(valueToPrepend, value...)
	mode = Prepend
	metaFoo = &MetaSetItem{Key: key, Value: valueToPrepend, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful prepend meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the add mode and existing key
	// will fail to store and return ErrNotStored error because key is in use
	mode = Add
	metaFoo = &MetaSetItem{Key: key, Value: []byte("add_value_to_existing"), Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("add mode with existing key meta set(%s) expected not stored error but got %e", key, err)
	}

	// set using the replace mode
	value = []byte("replacement_value")
	mode = Replace
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful replace mode meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the add mode
	// will store because key is not in use
	key = "new_key"
	value = []byte("add_value_to_new_key")
	mode = Add
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "add mode without existing key meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using base64 encoded string
	key = "bmV3QmFzZUtleQ=="
	decodedKey := "newBaseKey"
	value = []byte("with_base64_key")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{IsKeyBase64: true}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "base64 encoded key meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, decodedKey, value)

	// set using the append mode with non-existent key
	valueToAppend = []byte("new_append_value")
	key = "non_existing_for_append"
	mode = Append
	metaFoo = &MetaSetItem{Key: key, Value: valueToAppend, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Append with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}

	// set using the prepend mode with non-existent key
	valueToPrepend = []byte("new_prepend_value")
	key = "non_existing_for_prepend"
	mode = Prepend
	metaFoo = &MetaSetItem{Key: key, Value: valueToPrepend, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Prepend with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}

	// set using the replace mode with non-existent key
	value = []byte("new_replace_value")
	key = "non_existing_for_replace"
	mode = Replace
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: &mode}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Replace with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}
}

func testMetaDeleteCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	setForDelete := func(key string, value []byte, flags MetaSetFlags) *MetaResponseMetadata {
		metaSetItem := &MetaSetItem{Key: key, Value: value, Flags: flags}
		metaDataResponse, err := c.MetaSet(metaSetItem)
		checkErr(err, "meta set(%s): %v", key, err)
		return metaDataResponse
	}

	key := "foo_key"
	value := []byte("foo_val")
	setResponse := setForDelete(key, value, MetaSetFlags{ReturnCasTokenInResponse: true})
	casValue := setResponse.CasId

	// normal delete with key return and opaque token provided on matching CAS token
	opaqueToken := "opaque_token"
	metaDeleteItem := &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, OpaqueToken: &opaqueToken, CompareCasTokenToUpdateValue: casValue}}
	response, err := c.MetaDelete(metaDeleteItem)
	if *response.ItemKey != key {
		t.Errorf("meta delete(%s) Key = %q, want %s", key, *response.ItemKey, key)
	}
	if *response.OpaqueToken != opaqueToken {
		t.Errorf("meta delete(%s) Opaque token = %s, want %s", key, *response.OpaqueToken, opaqueToken)
	}
	checkErr(err, "meta delete(%s): %v", key, err)

	// failed delete due to mismatched CAS token
	var mismatchCasToken uint64 = 123456
	setResponse = setForDelete(key, value, MetaSetFlags{})
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, OpaqueToken: &opaqueToken, CompareCasTokenToUpdateValue: &mismatchCasToken}}
	_, err = c.MetaDelete(metaDeleteItem)
	if err != ErrCASConflict {
		t.Errorf("Different CAS token meta delete(%s) expected an CAS conflict error but got %e", key, err)
	}

	// delete with non-existent key
	key = "does_not_exist"
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{}}
	_, err = c.MetaDelete(metaDeleteItem)
	if err != ErrCacheMiss {
		t.Errorf("Non-existent key meta delete(%s) expected a cache miss error but got %e", key, err)
	}

	// delete with invalidation and TTL update- expect to see a new CAS token and TTL set after not being initially set
	setResponse = setForDelete(key, value, MetaSetFlags{ReturnCasTokenInResponse: true})
	getResponse, err := c.MetaGet(key, &MetaGetFlags{ReturnTTLRemainingSecondsInResponse: true})
	if getResponse.TTLRemainingInSeconds == nil || *getResponse.TTLRemainingInSeconds == 0 {
		t.Errorf("invalidation meta delete(%s) expected TTL to be non-nil and equal to 0", key)
	}
	checkErr(err, "meta get(%s): %v", key, err)
	casValue = setResponse.CasId
	var newTTL int32 = 5000
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{Invalidate: true, UpdateTTLToken: &newTTL}}
	response, err = c.MetaDelete(metaDeleteItem)
	checkErr(err, "meta delete(%s): %v", key, err)
	// after invalidation, the cas token should differ, so the CompareAndSwap method should raise an error when using the pre-delete CAS ID
	item := &Item{Key: key, Value: []byte("new_val_for_invalidation"), casid: *casValue}
	err = c.CompareAndSwap(item)
	if err != ErrCASConflict {
		t.Errorf("invalidation meta delete(%s) expected CAS error to be returned but got %e", key, err)
	}
	getResponse, err = c.MetaGet(key, &MetaGetFlags{ReturnTTLRemainingSecondsInResponse: true})
	if getResponse.TTLRemainingInSeconds == nil || *getResponse.TTLRemainingInSeconds == 0 {
		t.Errorf("invalidation meta delete(%s) expected TTL to be non-nil and greater than 0", key)
	}
	if !getResponse.IsItemStale {
		t.Errorf("invalidation meta delete(%s) expected item stale status to be true", key)
	}
	checkErr(err, "meta get(%s): %v", key, err)

	// delete with no-reply semantics
	// note that the documentation says that this flag will always return an error (even if the command runs successfully)
	setResponse = setForDelete(key, value, MetaSetFlags{})
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, UseNoReplySemanticsForResponse: true}}
	_, err = c.MetaDelete(metaDeleteItem)
	// the error raised is an internal error, so we can't check for explicit type
	if err == nil {
		t.Errorf("no reply meta delete(%s) expected an error to be returned but got none", key)
	}

	// delete with base-64 encoded key where decodedKey = newBaseKey
	key = "bmV3QmFzZUtleQ=="
	decodedKey := "newBaseKey"
	setResponse = setForDelete(key, value, MetaSetFlags{IsKeyBase64: true, ReturnKeyInResponse: true})
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{IsKeyBase64: true, ReturnKeyInResponse: true}}
	// check that the item was set with the properly decoded key before deleting
	it, err := c.Get(decodedKey)
	if it == nil {
		t.Errorf("base-64 encoded key meta set(%s) expected did not set", key)
	}
	checkErr(err, "get(%s): %v", key, err)
	// delete item
	response, err = c.MetaDelete(metaDeleteItem)
	if response.ItemKey == nil || *response.ItemKey != key {
		t.Errorf("base-64 encoded key meta delete(%s) expected key to be non-nil and %s", key, key)
	}
	checkErr(err, "base-64 encoded key meta delete(%s): %v", key, err)
	// check that the item is no longer able to be received with the decoded key post delete
	it, err = c.Get(decodedKey)
	if it != nil {
		t.Errorf("base-64 encoded key meta delete(%s) did not delete item", key)
	}
	if err != ErrCacheMiss {
		t.Errorf("base-64 encoded key meta delete(%s) expected cached miss error but got %v", key, err)
	}
}

func testMetaDebugCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	key := "test_key"
	value := []byte("debug_value")
	var timeToLive int32 = 5000
	flags := MetaSetFlags{ReturnCasTokenInResponse: true, UpdateTTLToken: &timeToLive}
	setResponse := metaSetForSetup(t, c, key, value, flags, checkErr)

	// debug call prior to fetch
	var size uint64
	metaDebugItem := &MetaDebugItem{Key: key}
	metaDebugResponse, err := c.MetaDebug(metaDebugItem)
	if metaDebugResponse.ExpirationTime == nil || *metaDebugResponse.ExpirationTime == -1 {
		t.Errorf("meta debug(%s) expected to have non-nil TTL and in the range [-5000, -1)", key)
	}
	if metaDebugResponse.LastAccessTime == nil {
		t.Errorf("meta debug(%s) expected to have non-nil time since last access", key)
	}
	if metaDebugResponse.CasId == nil || *metaDebugResponse.CasId != *setResponse.CasId {
		t.Errorf("meta debug(%s) expected to have non-nil CAS ID and have it equal to what it was when it was set", key)
	}
	if metaDebugResponse.Fetched {
		t.Errorf("meta debug(%s) expected to not have been fetched yet", key)
	}
	if metaDebugResponse.SlabClassId == nil || *metaDebugResponse.SlabClassId != 1 {
		t.Errorf("meta debug(%s) expected to have non-nil Slab Class ID and have it equal 1", key)
	}
	if metaDebugResponse.Size == nil || *metaDebugResponse.Size == 0 {
		t.Errorf("meta debug(%s) expected to have non-nil size and have it be greater than 0", key)
	} else {
		size = *metaDebugResponse.Size
	}
	checkErr(err, "meta debug(%s) immediately after set", key)

	// check that Fetched attribute is set to true after get
	c.Get(key)
	metaDebugResponse, _ = c.MetaDebug(metaDebugItem)
	if !metaDebugResponse.Fetched {
		t.Errorf("meta debug(%s) expected to have been fetched", key)
	}
	if metaDebugResponse.Size == nil || *metaDebugResponse.Size != size {
		t.Errorf("meta debug(%s) expected to have non-nil size and have it stay the same across calls", key)
	}

	// meta debug when value was not set with TTL
	metaSetForSetup(t, c, key, value, MetaSetFlags{}, checkErr)
	metaDebugItem = &MetaDebugItem{Key: key}
	metaDebugResponse, err = c.MetaDebug(metaDebugItem)
	if metaDebugResponse.ExpirationTime == nil || *metaDebugResponse.ExpirationTime != -1 {
		t.Errorf("meta debug(%s) expected to have non-nil TTL and to be -1", key)
	}

	// non-existent key meta debug call
	key = "non_existent_key"
	metaDebugItem = &MetaDebugItem{Key: key}
	_, err = c.MetaDebug(metaDebugItem)
	if err != ErrCacheMiss {
		t.Errorf("meta debug(%s) with non-existent key expected to have cache miss error but got %v", key, err)
	}
}

func metaSetForSetup(t *testing.T, c *Client, key string, value []byte, flags MetaSetFlags, checkErr func(err error, format string, args ...interface{})) *MetaResponseMetadata {
	metaSetItem := &MetaSetItem{Key: key, Value: value, Flags: flags}
	response, err := c.MetaSet(metaSetItem)
	checkErr(err, "meta set(%s): %v", key, err)
	return response
}

func testMetaSetSavedValue(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{}), key string, value []byte) {
	it, err := c.Get(key)
	checkErr(err, "get(%s): %v", key, err)
	if it.Key != key {
		t.Errorf("get(%s) Key = %q, want %s", key, it.Key, key)
	}
	if bytes.Compare(it.Value, value) != 0 {
		t.Errorf("get(%s) Value = %q, want %q", key, string(it.Value), string(value))
	}
}

func BenchmarkOnItem(b *testing.B) {
	fakeServer, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal("Could not open fake server: ", err)
	}
	defer fakeServer.Close()
	go func() {
		for {
			if c, err := fakeServer.Accept(); err == nil {
				go func() { io.Copy(ioutil.Discard, c) }()
			} else {
				return
			}
		}
	}()

	addr := fakeServer.Addr()
	c := New(addr.String())
	if _, err := c.getConn(addr); err != nil {
		b.Fatal("failed to initialize connection to fake server")
	}

	item := Item{Key: "foo"}
	dummyFn := func(_ *Client, _ *bufio.ReadWriter, _ *Item) error { return nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.onItem(&item, dummyFn)
	}
}
