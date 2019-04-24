## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *go get*

    $ go get github.com/michaeldistler/gomemcache/memcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/michaeldistler/gomemcache/memcache

## Example

    import (
            "github.com/michaeldistler/gomemcache/memcache"
    )

    func main() {
         // Select the algorithm and add IPs
         mc := memcache.New("ketama", "10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see (algorithm selection not included):

See https://godoc.org/github.com/bradfitz/gomemcache/memcache

Or run:

    $ godoc github.com/bradfitz/gomemcache/memcache

