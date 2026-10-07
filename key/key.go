package key

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"slices"

	"github.com/zeebo/blake3"
)

// Size is the fixed byte length of every key.
const Size = 32

// headerPos is the index of the header byte: the key's last byte. The layout
// is an Amber-Store key with its bytes reversed (architecture/keys.md), so the
// truncated hash leads and the header trails.
const headerPos = Size - 1

// Key is a 32-byte lookup key. It is a value type and is directly comparable,
// so it can be used as a Go map key. Accessors assume the key is canonical
// (produced by New, NewFromHash, or Parse).
type Key [Size]byte

// Type returns the CAS object type from the header's high nibble.
func (k Key) Type() Type {
	return Type(k[headerPos] >> 4)
}

// LengthSize returns the number of bytes the payload-length field occupies (1..8).
func (k Key) LengthSize() int {
	return int(k[headerPos]&0x07) + 1
}

// Length decodes the little-endian payload-length field.
func (k Key) Length() uint64 {
	ls := k.LengthSize()
	var buf [8]byte
	copy(buf[:ls], k[headerPos-ls:headerPos])
	return binary.LittleEndian.Uint64(buf[:])
}

// Hash returns the truncated payload hash bytes (len == Size-1-LengthSize) in
// digest order: the leading bytes of the BLAKE3 digest. The key stores them
// reversed, so this is a reordered copy, not a view of the key's prefix.
func (k Key) Hash() []byte {
	h := k[:headerPos-k.LengthSize()]
	slices.Reverse(h)
	return h
}

// lengthSizeFor returns the minimum number of bytes needed to hold length
// with no zero most-significant byte. Zero is the special case: a single 0x00 byte.
func lengthSizeFor(length uint64) int {
	if length == 0 {
		return 1
	}
	return (bits.Len64(length) + 7) / 8
}

// New computes the BLAKE3-256 digest of serialized, then assembles a canonical
// key via NewFromHash. length is the logical payload length and is taken as
// given (it need not equal len(serialized) — see NewFromHash).
func New(t Type, length uint64, serialized []byte) (Key, error) {
	return NewFromHash(t, length, blake3.Sum256(serialized))
}

// Validate reports whether k is canonical: the reserved bit is clear, the type
// is defined (0..5), and the length field is minimally encoded (its last, most
// significant byte is non-zero, except for the single 0x00 byte that encodes a
// zero length).
func (k Key) Validate() error {
	if k[headerPos]&0x08 != 0 {
		return ErrReservedBitSet
	}
	if !k.Type().IsValid() {
		return fmt.Errorf("%w: %d", ErrReservedType, uint8(k.Type()))
	}
	if k[headerPos-1] == 0 && !(k.LengthSize() == 1 && k.Length() == 0) {
		return ErrNonCanonicalLength
	}
	return nil
}

// Parse copies b into a Key and validates its canonical form. b must be exactly
// Size bytes.
func Parse(b []byte) (Key, error) {
	if len(b) != Size {
		return Key{}, fmt.Errorf("%w: got %d", ErrBadKeyLength, len(b))
	}
	var k Key
	copy(k[:], b)
	if err := k.Validate(); err != nil {
		return Key{}, err
	}
	return k, nil
}

// String returns the lowercase hex encoding of the key, for logs and errors.
func (k Key) String() string {
	return hex.EncodeToString(k[:])
}

// NewFromHash assembles a canonical key from a CAS object type, a logical
// payload length, and a precomputed full 256-bit BLAKE3 digest. The digest is
// truncated to its leading bytes, which fill the front of the key in reverse
// order. length is used verbatim: for Blob/XattrSet it is the serialized byte
// length; for FileNode/DirLeaf/DirNode and Commit it is a logical size (see
// architecture/types.md). Returns ErrReservedType if t is not a defined type.
func NewFromHash(t Type, length uint64, fullHash [Size]byte) (Key, error) {
	if !t.IsValid() {
		return Key{}, fmt.Errorf("%w: %d", ErrReservedType, uint8(t))
	}
	ls := lengthSizeFor(length)
	var k Key
	k[headerPos] = byte(t)<<4 | byte(ls-1)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], length)
	copy(k[headerPos-ls:headerPos], buf[:ls])
	hash := k[:headerPos-ls]
	copy(hash, fullHash[:])
	slices.Reverse(hash)
	return k, nil
}
