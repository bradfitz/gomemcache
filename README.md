## Brian's Fucking It Up...

Bradfitz's Golang Memcached client package is awesome, except when doing something goofy (like trying to deploy a memcached Compute Engine instance and make it talk to App Engine Standard -- what the hell, you ask? Trying to work around Cloud Functions' Node.js inability to talk to App Engine Memcache gracefully). This is my attempt to merge the App Engine Socket: http://google.golang.org/appengine/socket with Brad's masterpiece...

Viewer Discretion Advised - I have no clue what I'm doing.


(From the original):
## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *go get*

    $ go get github.com/bradfitz/gomemcache/memcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/bradfitz/gomemcache/memcache

## Example

    import (
            "github.com/bradfitz/gomemcache/memcache"
    )

    func main() {
         mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see:

See https://godoc.org/github.com/bradfitz/gomemcache/memcache

Or run:

    $ godoc github.com/bradfitz/gomemcache/memcache

