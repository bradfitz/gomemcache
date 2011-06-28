## About

This is a memcache client library for the Go programming language
(http://golang.org/).

## Installing

### Using *goinstall*

     $ goinstall github.com/bradfitz/gomemcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOROOT/src/pkg/github.com/bradfitz/gomemcache

You can use `goinstall -u -a` for update all installed packages.

### Using *git clone* command:

    $ git clone git://github.com/bradfitz/gomemcache
    $ cd gomemcache
    $ make install

## Example

    import (
            memcache "github.com/bradfitz/gomemcache"
    )

    func main() {
         mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211". "10.0.0.3:11212")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see:

   ...


