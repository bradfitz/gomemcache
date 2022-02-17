## About

This is a memcache client library derived from [gomemcache](https://github.com/bradfitz/gomemcache) for the Go programming language
(http://golang.org/).

## Why this project?

The version is bumped to v3 to indicate that it has something incompatible with the vanilla one.

### `get` vs `gets`

There are many derivate servers which implement **incomplete** memcache prototol, e.g. only `get` and `set` are implemented.

The thing is, the original repository of [gomemcache](https://github.com/bradfitz/gomemcache) has something confusing when it comes to [`get` command](https://github.com/bradfitz/gomemcache/blob/fb4bf637b56d66a1925c1bb0780b27dd714ec380/memcache/memcache.go#L361).

```go
if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
    return err
}
```

It means when you call `get`, the `gets` command is executed and [`casid` is always returned](https://github.com/bradfitz/gomemcache/blob/fb4bf637b56d66a1925c1bb0780b27dd714ec380/memcache/memcache.go#L523).

```go
dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
```

I've talked to bradfitz and got a lot of important advises from him. Truly all the things I mentioned above are all because of the **incomplete implementation** and have nothing to do with the client. What I stand for is `get` means `get` and `gets` means `gets`.

### `RoundRobinServerSelector`

Vanilla memcache server use crc32 to pick which server to store a key, for servers those distribute keys equally to all nodes the crc32 is not what is wanted. Thanks for bradfitz's brilliant work I can implement a `RoundRobinServerSelector` painlessly.

## Installing

### Using _go get_

`$ go get -u github.com/lovelock/gomemcache/v3/memcache`

After this command _gomemcache_ is ready to use. Its source will be in:

`$GOPATH/src/github.com/lovelock/gomemcache/memcache`

## Example

### For vanilla memcached server

```go
import (
    "github.com/lovelock/gomemcache/v3/memcache"
)

func main() {
    mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
    mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

    it, err := mc.Get("foo")
        ...
}
```

### For other derivatives

```go
import (
        "github.com/lovelock/gomemcache/v3/memcache"
)

func main() {
    mc := memcache.NewRoundRobin("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
    mc.DisableCAS = true // don't want get casid
    mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

    it, err := mc.Get("foo")
        ...
}
```

## Full docs, see:

See https://godoc.org/github.com/lovelock/gomemcache/v3/memcache

Or run:

`$ godoc github.com/lovelock/gomemcache/v3/memcache`
