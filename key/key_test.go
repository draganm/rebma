package key

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/zeebo/blake3"
)

func TestAccessors_SingleByteLength(t *testing.T) {
	// Blob, length 255 (lengthSize 1), 30-byte hash.
	var k Key
	k[31] = 0x00 // type 0, reserved 0, lengthSize-1 = 0
	k[30] = 0xFF // length = 255
	for i := 0; i < 30; i++ {
		k[i] = byte(i)
	}
	if k.Type() != Blob {
		t.Errorf("Type() = %v, want Blob", k.Type())
	}
	if k.LengthSize() != 1 {
		t.Errorf("LengthSize() = %d, want 1", k.LengthSize())
	}
	if k.Length() != 255 {
		t.Errorf("Length() = %d, want 255", k.Length())
	}
	if len(k.Hash()) != 30 {
		t.Errorf("len(Hash()) = %d, want 30", len(k.Hash()))
	}
	// The key holds the hash reversed; Hash() hands it back in digest order.
	want := slices.Clone(k[:30])
	slices.Reverse(want)
	if !bytes.Equal(k.Hash(), want) {
		t.Errorf("Hash() = %x, want %x", k.Hash(), want)
	}
}

func TestAccessors_MultiByteLength(t *testing.T) {
	// FileNode, length 65536 (lengthSize 3): header = (1<<4) | (3-1) = 0x12.
	var k Key
	k[31] = 0x12
	k[28], k[29], k[30] = 0x00, 0x00, 0x01 // little-endian 0x010000 = 65536
	if k.Type() != FileNode {
		t.Errorf("Type() = %v, want FileNode", k.Type())
	}
	if k.LengthSize() != 3 {
		t.Errorf("LengthSize() = %d, want 3", k.LengthSize())
	}
	if k.Length() != 65536 {
		t.Errorf("Length() = %d, want 65536", k.Length())
	}
	if len(k.Hash()) != 28 {
		t.Errorf("len(Hash()) = %d, want 28", len(k.Hash()))
	}
}

func TestNewFromHash_RoundTrip(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i + 1)
	}
	k, err := NewFromHash(DirNode, 1000, full)
	if err != nil {
		t.Fatal(err)
	}
	if k.Type() != DirNode {
		t.Errorf("Type() = %v, want DirNode", k.Type())
	}
	if k.Length() != 1000 {
		t.Errorf("Length() = %d, want 1000", k.Length())
	}
	if k.LengthSize() != 2 {
		t.Errorf("LengthSize() = %d, want 2", k.LengthSize())
	}
	if !bytes.Equal(k.Hash(), full[:Size-1-2]) {
		t.Errorf("Hash() truncation mismatch")
	}
}

func TestNewFromHash_LengthSizeBoundaries(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i)
	}
	cases := []struct {
		length uint64
		wantLS int
	}{
		{0, 1}, {1, 1}, {255, 1}, {256, 2}, {65535, 2}, {65536, 3},
		{1<<24 - 1, 3}, {1 << 24, 4}, {1<<32 - 1, 4}, {1 << 32, 5},
		{1 << 40, 6}, {1 << 48, 7}, {1<<56 - 1, 7}, {1 << 56, 8},
		{1<<64 - 1, 8},
	}
	for _, c := range cases {
		k, err := NewFromHash(Blob, c.length, full)
		if err != nil {
			t.Fatalf("length %d: %v", c.length, err)
		}
		if k.LengthSize() != c.wantLS {
			t.Errorf("length %d: LengthSize() = %d, want %d", c.length, k.LengthSize(), c.wantLS)
		}
		if k.Length() != c.length {
			t.Errorf("length %d: Length() = %d", c.length, k.Length())
		}
		wantHashLen := Size - 1 - c.wantLS
		if len(k.Hash()) != wantHashLen {
			t.Errorf("length %d: len(Hash()) = %d, want %d", c.length, len(k.Hash()), wantHashLen)
		}
		if !bytes.Equal(k.Hash(), full[:wantHashLen]) {
			t.Errorf("length %d: hash truncation mismatch", c.length)
		}
	}
}

func TestNewFromHash_ReservedType(t *testing.T) {
	var full [32]byte
	for _, ty := range []Type{6, 15, 16, 255} {
		if _, err := NewFromHash(ty, 1, full); !errors.Is(err, ErrReservedType) {
			t.Errorf("Type(%d): err = %v, want ErrReservedType", uint8(ty), err)
		}
	}
}

func TestNew_KnownAnswerAndTruncation(t *testing.T) {
	// Official BLAKE3-256 hash of the empty input.
	const wantHex = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	full := blake3.Sum256(nil)
	if got := hex.EncodeToString(full[:]); got != wantHex {
		t.Fatalf("blake3.Sum256(nil) = %s, want %s", got, wantHex)
	}
	// New(Blob, 0, nil): empty blob, lengthSize 1, hashLen 30.
	k, err := New(Blob, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if k.Length() != 0 || k.LengthSize() != 1 {
		t.Errorf("Length=%d LengthSize=%d, want 0 and 1", k.Length(), k.LengthSize())
	}
	if !bytes.Equal(k.Hash(), full[:30]) {
		t.Errorf("New hash truncation = %x, want %x", k.Hash(), full[:30])
	}
	// New must equal NewFromHash on the same content's digest.
	k2, _ := NewFromHash(Blob, 0, full)
	if k != k2 {
		t.Errorf("New != NewFromHash for the same content")
	}
}

func TestKey_DeterministicAndComparable(t *testing.T) {
	content := []byte("amber-store determinism check")
	a, _ := New(FileNode, uint64(len(content)), content)
	b, _ := New(FileNode, uint64(len(content)), content)
	if a != b {
		t.Fatal("New is not deterministic for identical inputs")
	}
	// Keys must be usable as Go map keys.
	m := map[Key]int{a: 1}
	if m[b] != 1 {
		t.Errorf("equal keys do not resolve to the same map entry")
	}
}

func TestNew_LengthIsLogicalNotSerialized(t *testing.T) {
	// The length field is the logical payload size and is passed verbatim; it is
	// NOT derived from or validated against len(serialized). A FileNode covering
	// a 1 MiB file region has length 1<<20 even though its own serialized bytes
	// (here stand-in content) are tiny. The hash still covers the serialized
	// bytes, not the logical length.
	content := []byte("tiny")
	k, err := New(FileNode, 1<<20, content)
	if err != nil {
		t.Fatal(err)
	}
	if k.Length() != 1<<20 {
		t.Errorf("Length() = %d, want %d", k.Length(), 1<<20)
	}
	full := blake3.Sum256(content)
	if !bytes.Equal(k.Hash(), full[:len(k.Hash())]) {
		t.Errorf("hash must cover the serialized bytes, not the logical length")
	}
}

func TestParse_RoundTrip(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i + 1)
	}
	for _, ty := range []Type{Blob, FileNode, DirLeaf, DirNode, XattrSet} {
		k, _ := NewFromHash(ty, 12345, full)
		got, err := Parse(k[:])
		if err != nil {
			t.Fatalf("%v: Parse: %v", ty, err)
		}
		if got != k {
			t.Errorf("%v: round-trip mismatch", ty)
		}
	}
}

func TestParse_BadLength(t *testing.T) {
	for _, n := range []int{0, 31, 33, 64} {
		if _, err := Parse(make([]byte, n)); !errors.Is(err, ErrBadKeyLength) {
			t.Errorf("len %d: err = %v, want ErrBadKeyLength", n, err)
		}
	}
}

func TestValidate_ReservedBit(t *testing.T) {
	var full [32]byte
	k, _ := NewFromHash(Blob, 1, full)
	k[31] |= 0x08 // set the reserved bit
	if err := k.Validate(); !errors.Is(err, ErrReservedBitSet) {
		t.Errorf("err = %v, want ErrReservedBitSet", err)
	}
}

func TestValidate_ReservedType(t *testing.T) {
	var k Key
	k[31] = 6 << 4 // type 6, lengthSize 1
	k[30] = 0x01
	if err := k.Validate(); !errors.Is(err, ErrReservedType) {
		t.Errorf("err = %v, want ErrReservedType", err)
	}
}

func TestValidate_NonCanonicalLength(t *testing.T) {
	// Blob, lengthSize 2 (header low bits = 1), length bytes 0x05 0x00
	// (little-endian): a zero most-significant byte with a non-zero value ->
	// non-canonical.
	var k Key
	k[31] = 0x01
	k[29], k[30] = 0x05, 0x00
	if err := k.Validate(); !errors.Is(err, ErrNonCanonicalLength) {
		t.Errorf("err = %v, want ErrNonCanonicalLength", err)
	}
}

func TestValidate_ZeroLengthIsCanonical(t *testing.T) {
	// Blob, lengthSize 1, length byte 0x00: value 0 is the allowed special case.
	var k Key // all zero bytes
	if err := k.Validate(); err != nil {
		t.Errorf("zero-length key should validate, got %v", err)
	}
}

func TestString_Hex(t *testing.T) {
	var k Key
	k[0] = 0xFF
	k[31] = 0x12
	got := k.String()
	if want := hex.EncodeToString(k[:]); got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}
	if len(got) != 2*Size {
		t.Errorf("len(String()) = %d, want %d", len(got), 2*Size)
	}
}

func TestNewFromHash_Commit(t *testing.T) {
	var full [32]byte
	k, err := NewFromHash(Commit, 100, full)
	if err != nil {
		t.Fatal(err)
	}
	if k[31] != 0x50 {
		t.Errorf("header byte = %#x, want 0x50 (type 5, one length byte)", k[31])
	}
	if k.Type() != Commit || k.Length() != 100 {
		t.Errorf("Type() = %v, Length() = %d; want Commit, 100", k.Type(), k.Length())
	}
	if err := k.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// amberKey assembles the Amber-Store layout this package reverses: header
// byte, big-endian payload length, then the leading bytes of the digest.
func amberKey(t Type, length uint64, full [Size]byte) [Size]byte {
	ls := lengthSizeFor(length)
	var a [Size]byte
	a[0] = byte(t)<<4 | byte(ls-1)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], length)
	copy(a[1:1+ls], buf[8-ls:])
	copy(a[1+ls:], full[:Size-1-ls])
	return a
}

func TestNewFromHash_IsReversedAmberKey(t *testing.T) {
	full := blake3.Sum256([]byte("rebma"))
	lengths := []uint64{0, 1, 255, 256, 65536, 1<<24 - 1, 1 << 32, 1 << 40, 1 << 48, 1 << 56, 1<<64 - 1}
	for _, ty := range []Type{Blob, FileNode, DirLeaf, DirNode, XattrSet, Commit} {
		for _, length := range lengths {
			k, err := NewFromHash(ty, length, full)
			if err != nil {
				t.Fatalf("%v, length %d: %v", ty, length, err)
			}
			want := amberKey(ty, length, full)
			slices.Reverse(want[:])
			if k != Key(want) {
				t.Errorf("%v, length %d: key = %x, want %x", ty, length, k[:], want[:])
			}
			if err := k.Validate(); err != nil {
				t.Errorf("%v, length %d: Validate: %v", ty, length, err)
			}
		}
	}
}

func TestNewFromHash_KnownLayout(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i + 1)
	}
	// DirNode (3), length 1000 = 0x03e8 (lengthSize 2): 29 digest bytes
	// reversed, the length little-endian, the header byte last.
	k, err := NewFromHash(DirNode, 1000, full)
	if err != nil {
		t.Fatal(err)
	}
	const want = "1d1c1b1a191817161514131211100f0e0d0c0b0a090807060504030201" + "e803" + "31"
	if got := k.String(); got != want {
		t.Errorf("key = %s, want %s", got, want)
	}
}
