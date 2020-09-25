package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const TextProtoType = "text"

var textCommander = &textCmdRunner{}

type textCmdRunner struct{}

func (pcr *textCmdRunner) ProtoType() string {
	return TextProtoType
}

func (pcr *textCmdRunner) IsAuthSupported() bool {
	return false
}
func (pcr *textCmdRunner) Auth(*bufio.ReadWriter, string, string) error {
	return errors.New("method Auth is not implemented for plain cmd runner")
}
func (pcr *textCmdRunner) GetCmd(rw *bufio.ReadWriter, keys []string, cb func(*Item)) error {
	if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	if err := pcr.parseGetResponse(rw.Reader, cb); err != nil {
		return err
	}
	return nil
}
func (pcr *textCmdRunner) PopulateCmd(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}
	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err := rw.Write(crlf); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func (pcr *textCmdRunner) Delete(rw *bufio.ReadWriter, key string) error {
	return pcr.writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
}

func (pcr *textCmdRunner) DeleteAll(rw *bufio.ReadWriter) error {
	return pcr.writeExpectf(rw, resultDeleted, "flush_all\r\n")
}

func (pcr *textCmdRunner) FlushAll(rw *bufio.ReadWriter) error {
	return pcr.writeExpectf(rw, resultOK, "flush_all\r\n")
}

func (pcr *textCmdRunner) writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := pcr.writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

func (pcr *textCmdRunner) Ping(rw *bufio.ReadWriter) error {
	if _, err := fmt.Fprintf(rw, "version\r\n"); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}

	switch {
	case bytes.HasPrefix(line, versionPrefix):
		break
	default:
		return fmt.Errorf("memcache: unexpected response line from ping: %q", string(line))
	}
	return nil
}

func (pcr *textCmdRunner) Touch(rw *bufio.ReadWriter, keys []string, expiration int32) error {
	for _, key := range keys {
		if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultTouched):
			break
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		default:
			return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
		}
	}
	return nil
}

func (pcr *textCmdRunner) IncrDecrCmd(rw *bufio.ReadWriter, verb, key string, delta uint64) (uint64, error) {
	line, err := pcr.writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
	if err != nil {
		return 0, err
	}
	switch {
	case bytes.Equal(line, resultNotFound):
		return 0, ErrCacheMiss
	case bytes.HasPrefix(line, resultClientErrorPrefix):
		errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
		return 0, errors.New("memcache: client error: " + string(errMsg))
	}
	val, err := strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (pcr *textCmdRunner) writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	line, err := rw.ReadSlice('\n')
	return line, err
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func (pcr *textCmdRunner) scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	if bytes.Count(line, space) == 3 {
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func (pcr *textCmdRunner) parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		it := new(Item)
		size, err := pcr.scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}
