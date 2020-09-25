package memcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

const BinaryProtoType = "binary"

var binCommander = &binCmdRunner{}

type binCmdRunner struct{}

func (bcr *binCmdRunner) ProtoType() string {
	return BinaryProtoType
}

func (bcr *binCmdRunner) IsAuthSupported() bool {
	return true
}

func (bcr *binCmdRunner) Auth(rw *bufio.ReadWriter, username, password string) error {
	s, err := bcr.authList(rw)
	if err != nil {
		return err
	}

	switch {
	case strings.Index(s, "PLAIN") != -1:
		return bcr.authPlain(rw, username, password)
	}

	return fmt.Errorf("memcache: unknown auth types %q", s)
}

func (bcr *binCmdRunner) authPlain(rw *bufio.ReadWriter, username, password string) error {
	m := &msg{
		header: header{
			Op: opAuthStart,
		},

		key: "PLAIN",
		val: []byte(fmt.Sprintf("\x00%s\x00%s", username, password)),
	}

	return binSendRecv(rw, m)
}

func (bcr *binCmdRunner) authList(rw *bufio.ReadWriter) (string, error) {
	m := &msg{
		header: header{
			Op: opAuthList,
		},
	}

	err := binSendRecv(rw, m)
	return string(m.val), err
}

func (bcr *binCmdRunner) GetCmd(rw *bufio.ReadWriter, keys []string, cb func(*Item)) error {
	if len(keys) != 1 {
		return fmt.Errorf("memcached: pipelining and multiple keys get are not supported for now")
	}
	var flags uint32
	m := &msg{
		header: header{
			Op:  opGet,
			CAS: uint64(0),
		},
		oextras: []interface{}{&flags},
		key:     keys[0],
	}
	err := binSendRecv(rw, m)
	if err != nil {
		return err
	}
	cb(&Item{
		Key:   keys[0],
		Value: m.val,
		casid: m.CAS,
		Flags: flags,
	})
	return nil
}

func (bcr *binCmdRunner) PopulateCmd(rw *bufio.ReadWriter, verb string, item *Item) error {
	var ocas uint64
	var op opCode
	switch verb {
	case "cas":
		op = opSet
		ocas = item.casid
	case "set":
		op = opSet
	case "add":
		op = opAdd
	case "replace":
		op = opReplace
	}

	m := &msg{
		header: header{
			Op:  op,
			CAS: ocas,
		},
		iextras: []interface{}{item.Flags, uint32(item.Expiration)},
		key:     item.Key,
		val:     item.Value,
	}

	err := binSendRecv(rw, m)
	if err == ErrCASConflict && verb == "add" || verb == "replace" {
		return ErrNotStored
	}
	return err
}

func (bcr *binCmdRunner) Delete(rw *bufio.ReadWriter, key string) error {
	return bcr.DeleteCas(rw, key, 0)
}

func (bcr *binCmdRunner) DeleteCas(rw *bufio.ReadWriter, key string, cas uint64) error {
	m := &msg{
		header: header{
			Op:  opDelete,
			CAS: cas,
		},
		key: key,
	}
	return binSendRecv(rw, m)
}

func (bcr *binCmdRunner) DeleteAll(rw *bufio.ReadWriter) error {
	m := &msg{
		header: header{
			Op: opFlush,
		},
	}
	return binSendRecv(rw, m)
}

func (bcr *binCmdRunner) FlushAll(rw *bufio.ReadWriter) error {
	return bcr.DeleteAll(rw)
}

func (bcr *binCmdRunner) Ping(rw *bufio.ReadWriter) error {
	m := &msg{
		header: header{
			Op: opVersion,
		},
	}

	return binSendRecv(rw, m)
}

func (bcr *binCmdRunner) Touch(rw *bufio.ReadWriter, keys []string, expiration int32) error {
	exp := uint32(expiration)
	m := &msg{
		header: header{
			Op: opTouch,
		},
		iextras: []interface{}{exp},
	}

	for _, key := range keys {
		m.key = key
		err := binSendRecv(rw, m)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bcr *binCmdRunner) IncrDecrCmd(rw *bufio.ReadWriter, verb, key string, delta uint64) (uint64, error) {
	var op opCode
	switch verb {
	case "incr":
		op = opIncrement
	case "decr":
		op = opDecrement
	}

	var init uint64
	var exp uint32 = 0xffffffff

	m := &msg{
		header: header{
			Op: op,
		},
		iextras: []interface{}{delta, init, exp},
		key:     key,
	}

	err := binSendRecv(rw, m)
	if err != nil {
		return 0, err
	}
	val, err := binReadInt(string(m.val))
	return val, nil
}

func binReadInt(b string) (uint64, error) {
	switch len(b) {
	case 8: // 64 bit
		return uint64(uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
			uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56), nil
	}

	return 0, fmt.Errorf("memcache: error parsing int %s", b)
}

func binSend(rw *bufio.ReadWriter, m *msg) error {
	m.Magic = magicSend
	m.ExtraLen = sizeOfExtras(m.iextras)
	m.KeyLen = uint16(len(m.key))
	m.BodyLen = uint32(m.ExtraLen) + uint32(m.KeyLen) + uint32(len(m.val))
	// m.Opaque = sc.opq
	// sc.opq++

	b := bytes.NewBuffer(nil)
	// Request
	err := binary.Write(b, binary.BigEndian, m.header)
	if err != nil {
		return err
	}

	for _, e := range m.iextras {
		err = binary.Write(b, binary.BigEndian, e)
		if err != nil {
			return err
		}
	}

	_, err = io.WriteString(b, m.key)
	if err != nil {
		return err
	}

	_, err = b.Write(m.val)
	if err != nil {
		return err
	}

	_, err = rw.Write(b.Bytes())
	if err != nil {
		return err
	}
	return rw.Flush()
}

func binRecv(r *bufio.Reader, m *msg) error {
	err := binary.Read(r, binary.BigEndian, &m.header)
	if err != nil {
		return err
	}

	bd := make([]byte, m.BodyLen)
	_, err = io.ReadFull(r, bd)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(bd)

	if m.ResvOrStatus == 0 && m.ExtraLen > 0 {
		for _, e := range m.oextras {
			err := binary.Read(buf, binary.BigEndian, e)
			if err != nil {
				return err
			}
		}
	}

	m.key = string(buf.Next(int(m.KeyLen)))
	vlen := int(m.BodyLen) - int(m.ExtraLen) - int(m.KeyLen)
	m.val = buf.Next(int(vlen))
	return newErrorFromBinary(m.ResvOrStatus)
}

func binSendRecv(rw *bufio.ReadWriter, m *msg) error {
	err := binSend(rw, m)
	if err != nil {
		return err
	}

	return binRecv(rw.Reader, m)
}

// sizeOfExtras returns the size of the extras field for the memcache request.
func sizeOfExtras(extras []interface{}) (l uint8) {
	for _, e := range extras {
		switch e.(type) {
		default:
			panic(fmt.Sprintf("mc: unknown extra type (%T)", e))
		case uint8:
			l += 8 / 8
		case uint16:
			l += 16 / 8
		case uint32:
			l += 32 / 8
		case uint64:
			l += 64 / 8
		}
	}
	return
}
