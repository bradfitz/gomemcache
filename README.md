## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Example

Install with:

```shell
$ go get github.com/bradfitz/gomemcache/memcache
```

Then use it like:

```go
import (
    "github.com/bradfitz/gomemcache/memcache"
)

func main() {
     mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
     mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

     it, err := mc.Get("foo")
     ...
}
```

## Disabling CAS Support

By default, the client uses `gets` commands to support CAS (Compare-And-Swap) operations.
If your memcached setup does not support CAS or you want to disable it (e.g., for performance or compatibility),
you can set the `DisableCAS` field on the client:

```go
mc := memcache.New("127.0.0.1:11211")
mc.DisableCAS = true
```

When `DisableCAS` is enabled:
- The client will use `get` instead of `gets` when fetching items.
- CAS-based methods (like `CompareAndSwap`) will either be no-ops or fall back to simpler operations (e.g., `Set`).

## Full docs, see:

See https://pkg.go.dev/github.com/bradfitz/gomemcache/memcache

Or run:

```shell
$ godoc github.com/bradfitz/gomemcache/memcache
```
