package memcache

// Status Codes that may be returned (usually as part of an Error).
const (
	BinStatusOK             = uint16(0)
	BinStatusNotFound       = uint16(1)
	BinStatusKeyExists      = uint16(2)
	BinStatusValueTooLarge  = uint16(3)
	BinStatusInvalidArgs    = uint16(4)
	BinStatusValueNotStored = uint16(5)
	BinStatusNonNumeric     = uint16(6)
	BinStatusAuthRequired   = uint16(0x20)
	BinStatusAuthContinue   = uint16(0x21)
	BinStatusUnknownCommand = uint16(0x81)
	BinStatusOutOfMemory    = uint16(0x82)
	BinStatusAuthUnknown    = uint16(0xffff)
	BinStatusNetworkError   = uint16(0xfff1)
	BinStatusUnknownError   = uint16(0xffff)
)

// newErrorFromBinary takes a status from the server and creates a matching Error.
func newErrorFromBinary(status uint16) error {
	switch status {
	case BinStatusOK:
		return nil
	case BinStatusNotFound:
		return ErrCacheMiss
	case BinStatusKeyExists:
		return ErrCASConflict
	case BinStatusValueTooLarge:
		return ErrValueTooLarge
	case BinStatusInvalidArgs:
		return ErrInvalidArgs
	case BinStatusValueNotStored:
		return ErrValueNotStored
	case BinStatusNonNumeric:
		return ErrNonNumeric
	case BinStatusAuthRequired:
		return ErrAuthRequired

	// we only support PLAIN auth, no mechanism that would make use of auth
	// continue, so make it an error for now for completeness.
	case BinStatusAuthContinue:
		return ErrAuthContinue
	case BinStatusUnknownCommand:
		return ErrUnknownCommand
	case BinStatusOutOfMemory:
		return ErrOutOfMemory
	}
	return ErrUnknownError
}

type opCode uint8

// ops
const (
	opGet opCode = opCode(iota)
	opSet
	opAdd
	opReplace
	opDelete
	opIncrement
	opDecrement
	opQuit
	opFlush
	opGetQ
	opNoop
	opVersion
	opGetK
	opGetKQ
	opAppend
	opPrepend
	opStat
	opSetQ
	opAddQ
	opReplaceQ
	opDeleteQ
	opIncrementQ
	opDecrementQ
	opQuitQ
	opFlushQ
	opAppendQ
	opPrependQ
	opVerbosity // verbosity - not implemented in memcached (but other servers)
	opTouch
	opGAT
	opGATQ
	opGATK  = opCode(0x23)
	opGATKQ = opCode(0x24)
)

// Auth Ops
const (
	opAuthList opCode = opCode(iota + 0x20)
	opAuthStart
	opAuthStep
)

// Magic Codes
type binMagicCode uint8

const (
	magicSend binMagicCode = 0x80
	magicRecv binMagicCode = 0x81
)

// Memcache header
type header struct {
	Magic        binMagicCode
	Op           opCode
	KeyLen       uint16
	ExtraLen     uint8
	DataType     uint8  // not used, memcached expects it to be 0x00.
	ResvOrStatus uint16 // for request this field is reserved / unused, for
	// response it indicates the status
	BodyLen uint32
	Opaque  uint32 // copied back to you in response message (message id)
	CAS     uint64 // version really
}

// Main Memcache message structure
type msg struct {
	header                // [0..23]
	iextras []interface{} // [24..(m-1)] Command specific extras (In)

	// Idea of this is we can pass in pointers to values that should appear in the
	// response extras in this field and the generic send/recieve code can handle.
	oextras []interface{} // [24..(m-1)] Command specifc extras (Out)

	key string // [m..(n-1)] Key (as needed, length in header)
	val []byte // [n..x] Value (as needed, length in header)
}

// Memcache stats
type mcStats map[string]string
