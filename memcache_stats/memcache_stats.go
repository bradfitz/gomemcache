package main

import (
	"flag"
	"fmt"

	"github.com/ungerik/gomemcache/memcache"
)

var server = flag.String("server", "localhost:11211", "server address")

func main() {
	c := memcache.New(*server)
	stats, err := c.Stats(0)
	if err != nil {
		panic(err)
	}
	fmt.Println("memcache stats from", *server)
	for key, value := range stats {
		fmt.Printf("%s: %s\n", key, value)
	}
}
