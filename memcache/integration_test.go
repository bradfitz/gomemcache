// +build integration

package memcache

import (
	"context"
	"testing"
)

func TestCommands(t *testing.T) {
	var (
		item1 = Item{
			Key:   "foo",
			Value: []byte("bar"),
		}
		item2 = Item{
			Key:   "foo2",
			Value: []byte("bar2"),
		}

		c = New("localhost:11211")
	)
	err := c.Ping()
	if err != nil {
		t.Fatalf("memcached server isn't running: %s", err.Error())
	}
	err = c.DeleteAll()
	if err != nil {
		t.Fatalf("can't clean up memcached before tests: %s", err.Error())
	}

	t.Run("Test_SetMultiV2", func(t *testing.T) {
		err := c.SetMultiV2(context.TODO(), []*Item{&item1, &item2})
		if err != nil {
			t.Fatalf("can't set test data to memcached: %s", err.Error())
		}

		keys := []string{item1.Key, item2.Key}

		resp, err := c.GetMulti(keys)
		if err != nil {
			t.Fatalf("can't get test data from memcached: %s", err.Error())
		}

		if len(resp) < len(keys) {
			t.Fatalf("not enough items received: expected %d, got %d", len(keys), len(resp))
		}

		var (
			i1 = resp[item1.Key]
			i2 = resp[item2.Key]
		)

		if string(i1.Value) != string(item1.Value) {
			t.Fatalf("can't read correct data for item1: %v", i1.Value)
		}

		if string(i2.Value) != string(item2.Value) {
			t.Fatalf("can't read correct data for item2: %v", i2.Value)
		}

	})

	t.Run("Test_DeleteMulti", func(t *testing.T) {
		t.Skip()
		err = c.DeleteMulti([]string{item1.Key, item2.Key})
		if err != nil {
			t.Fatalf("can't delete data: %s", err.Error())
		}

		_, err = c.Get(item1.Key)
		if err != ErrCacheMiss {
			t.Fatalf("data doesn't deleted after command for key: %s", err.Error())
		}
		_, err = c.Get(item2.Key)
		if err != ErrCacheMiss {
			t.Fatalf("data doesn't deleted after command for key2: %s", err.Error())
		}
	})
}
