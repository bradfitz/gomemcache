## Brian's F*#%ing It Up...

Using Brad Fitzpatrick's code (https://github.com/bradfitz/gomemcache) and making it work on App Engine Standard Environment

Bradfitz's Golang Memcached client package is awesome, except when doing something goofy (like trying to deploy a memcached Compute Engine instance and make it talk to App Engine Standard -- what the hell, you ask? Trying to work around Cloud Functions' Node.js inability to talk to App Engine Memcache gracefully). This is my attempt to merge the App Engine Socket: http://google.golang.org/appengine/socket with Brad's masterpiece...


(From Brad's original changed links to this package though):
## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *go get*

    $ go get github.com/briankovacs-roi/gomemcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/briankovacs-roi/gomemcache

## Example

    import (
            "github.com/briankovacs-roi/gomemcache"
    )

    func main() {
         mc := memcache.New(app_eng_context, "10.0.0.1:11211")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see:

See https://godoc.org/github.com/bradfitz/gomemcache/memcache
***Based on Brad's work above, look for minor changes in appengstd_memcache.go as commented

Or run:

    $ godoc github.com/bradfitz/gomemcache/memcache

