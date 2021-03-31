// +build integration

package memcache

import (
	"context"
	"testing"
)

func TestDeleteMulti(t *testing.T) {
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
		t.Skip("memcached server doesn't run")
	}
	err = c.DeleteAll()
	if err != nil {
		t.Fatal("can't clean up memcached before tests")
	}

	t.Run("Test_SetMulti", func(t *testing.T) {
		err := c.SetMulti(context.TODO(), []*Item{&item1, &item2})
		if err != nil {
			t.Fatal("can't set test data to memcached")
		}

		resp, err := c.GetMulti([]string{item1.Key, item2.Key})
		if err != nil {
			t.Fatal("can't get test data from memcached")
		}
		var (
			i1 = resp[item1.Key]
			i2 = resp[item2.Key]
		)
		if i1 == nil || string(i1.Value) != string(item1.Value) {
			t.Fatal("can't read correct data for item1")
		}
		if i2 == nil || string(i2.Value) != string(item2.Value) {
			t.Fatal("can't read correct data for item2")
		}

	})

	t.Run("Test_DeleteMulti", func(t *testing.T) {
		err = c.DeleteMulti([]string{item1.Key, item2.Key})
		if err != nil {
			t.Fatal("can't delete data")
		}

		_, err = c.Get(item1.Key)
		if err != ErrCacheMiss {
			t.Fatal("data doesn't deleted after command for key")
		}
		_, err = c.Get(item2.Key)
		if err != ErrCacheMiss {
			t.Fatal("data doesn't deleted after command for key2")
		}
	})
}
