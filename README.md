## About

**IMPORTANT**
THIS IS A FORK OF https://github.com/bradfitz/gomemcache.
I CAN'T USE THE ORIGINAL ONE BECAUSE GETS AND GET CONFUSION. v1.0.0 UNIFIED THE ISSUE.

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *go mod*

    $ go mod edit -require github.com/lovelock/gomemcache/memcache@v1.0.1

### Using *go get*

    $ go get github.com/lovelock/gomemcache/memcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/lovelock/gomemcache/memcache

## Example

    import (
            "github.com/lovelock/gomemcache/memcache"
    )

    func main() {
         mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see:

See https://godoc.org/github.com/lovelock/gomemcache/memcache

Or run:

    $ godoc github.com/lovelock/gomemcache/memcache

