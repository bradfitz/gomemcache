## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *go get*

    $ go get github.com/orijtech/gomemcache/memcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/orijtech/gomemcache/memcache

## Example

    import (
            "context"

            "github.com/orijtech/gomemcache/memcache"
    )

    func main() {
         mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
        ctx := context.Background()
         mc.Set(ctx, &memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get(ctx, "foo")
         ...
    }

## Example with distributed tracing and monitoring enabled

    import (
        "context"

        "github.com/orijtech/gomemcache/memcache"

        "go.opencensus.io/trace"
        "go.opencensus.io/stats/view"
    )

    func main() {
        if err := view.Register(memcache.AllViews...); err != nil {
            log.Fatalf("Failed to register the monitoring views: %v", err)
        }

        ctx := context.Background()

        mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
        mc.Set(ctx, &memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get(ctx, "foo")
         ...
    }

## Full docs, see:

See https://godoc.org/github.com/orijtech/gomemcache/memcache

Or run:

    $ godoc github.com/orijtech/gomemcache/memcache

