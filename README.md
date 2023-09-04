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

## Full docs, see:

See https://pkg.go.dev/github.com/bradfitz/gomemcache/memcache

Or run:

```shell
$ godoc github.com/bradfitz/gomemcache/memcache
```

