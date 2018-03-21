package memcache

import (
	"testing"
)

func TestServerMisBehaving(t *testing.T) {
	servers := []string{"localhost:22133"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}

	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < errCount; i++ {
		addr, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}

		client.selector.ServerMisbehaving()
	}
	_, _, err = client.selector.PickQueue()

	if err != ErrNoServers {
		t.Error("Failed to return ErrNoServers error")
	}
}

func TestErrNoServersSingle(t *testing.T) {
	servers := []string{"localhost:22133"}
	queue := []string{"ns__queueName__env"}
	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount; i++ {
		_, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}
		client.selector.QueueCacheMiss()
	}

	_, _, err = client.selector.PickQueue()

	if err != ErrNoServers {
		t.Error("Failed to return ErrNoServers error")
	}
}

func TestErrNoServers(t *testing.T) {
	servers := []string{"localhost:22133", "localhost:22134"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}
	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount*4; i++ {
		_, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}
		client.selector.QueueCacheMiss()
	}

	_, _, err = client.selector.PickQueue()

	if err != ErrNoServers {
		t.Error("Failed to return ErrNoServers")
	}
}

func TestNoMissSingle(t *testing.T) {
	servers := []string{"localhost:22133"}
	queue := []string{"ns__queueName__env"}
	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount-1; i++ {
		_, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}
		client.selector.QueueCacheMiss()
	}

	_, _, err = client.selector.PickQueue()

	if err != nil {
		t.Errorf("Unexpected err=%s\n", err.Error())
	}
}

func TestNoMiss(t *testing.T) {
	servers := []string{"localhost:22133", "localhost:22134"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}
	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount*4-1; i++ {
		_, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}
		client.selector.QueueCacheMiss()
	}

	_, _, err = client.selector.PickQueue()

	if err != nil {
		t.Errorf("Unexpected err=%s\n", err.Error())
	}
}

func TestNoMissWithHit(t *testing.T) {
	servers := []string{"localhost:22133", "localhost:22134"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}
	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount*4-1; i++ {
		_, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}
		client.selector.QueueCacheMiss()
	}

	client.selector.PickQueue()
	client.selector.QueueCacheHit()
	_, _, err = client.selector.PickQueue()

	if err != nil {
		t.Errorf("Unexpected err=%s\n", err.Error())
	}
}

func TestQueueOrder(t *testing.T) {
	servers := []string{"localhost:22133"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}

	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	//t.Logf("queues=%v\n", client.selector.GetQueues())

	for i := 0; i < 4; i++ {
		_, key, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}

		// We select lastQueueIndex+1
		if key != queue[(i+1)%2] {
			t.Errorf("Expected queue=%s, but get queue=%s\n", queue[i%2], key)
		}

		client.selector.QueueCacheHit()
	}
}

func TestServerOrder(t *testing.T) {
	servers := []string{"localhost:22133", "localhost:22134"}
	queue := []string{"ns__queueName__env", "ns__queueName__subq__env"}

	client, err := NewWithQueues(servers, queue)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < missCount*len(queue); i++ {
		addr, _, err := client.selector.PickQueue()
		if err != nil {
			t.Errorf("Unexpected err=%s\n", err.Error())
		}

		if addr.String() != "127.0.0.1:22133" {
			t.Errorf("Picked server=%s, expected=127.0.0.1:22133\n", addr.String())
		}
		client.selector.QueueCacheHit()
	}
}
