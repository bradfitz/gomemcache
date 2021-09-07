# gomemcache
This is a [memcache](https://memcached.org/) client library for the [Go programming language](http://golang.org/).

## Installing

### Using *go get*

```bash
go get github.com/bradfitz/gomemcache/memcache
```

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/bradfitz/gomemcache/memcache

## Example
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
## API docs
See https://godoc.org/github.com/bradfitz/gomemcache/memcache

Or run:

```bash
godoc github.com/bradfitz/gomemcache/memcache
```

## Development
If you fork the lib, you're likely to have problem using it in your project. For that, use something like:

```bash
go mod edit -replace="github.com/bradfitz/gomemcache=github.com/xxxxxxxx/gomemcache"
go mod tidy
```

## Testing
Tests require memcache server to run on localhost
```
go test
```
