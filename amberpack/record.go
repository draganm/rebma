package amberpack

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/draganm/rebma/key"
	"github.com/klauspost/compress/zstd"
)

const (
	// RecHeaderSize is the fixed record-header length:
	// tag(1) + key(32) + flags(1) + ulen(4) + slen(4) + crc(4). Payload follows.
	RecHeaderSize = 46

	tagChunk byte = 0x01
	flagZstd byte = 0x01

	// MaxPayload bounds one object's payload, stored or decoded. The length
	// fields are untrusted and size allocations. Real objects are ~1 MiB.
	MaxPayload = 256 << 20
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	zero4      [4]byte
)

// ErrCorrupt wraps every record-level corruption error surfaced by ParseRecord
// and DecodePayload (bad framing, bad flags, CRC mismatch, length
// inconsistency, non-canonical key). It is the record-level counterpart to the
// stream-level ErrMalformed; distinguish either with errors.Is. The packstore
// package aliases this sentinel for its footer- and scrub-level corruption too,
// so the message stays deliberately general rather than naming "record".
var ErrCorrupt = errors.New("amberpack: corrupt pack data")

// Record describes a parsed record header. The payload lives at
// [RecHeaderSize : RecHeaderSize+Slen] within the record's bytes.
type Record struct {
	Key key.Key
	// Flags is the raw record flag byte; pass it to DecodePayload unchanged.
	Flags byte
	Ulen  uint32
	Slen  uint32
}

// Shared zstd coders; EncodeAll/DecodeAll are safe for concurrent use.
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	if zstdEnc, err = zstd.NewWriter(nil); err != nil {
		panic(err)
	}
	// CapLimit stops DecodeAll at the dst capacity DecodePayload sizes from ulen.
	if zstdDec, err = zstd.NewReader(nil, zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(MaxPayload)); err != nil {
		panic(err)
	}
}

// EncodeRecord serializes (k, data) into a complete record, compressing the
// payload with zstd when that makes it strictly smaller. k is written as given;
// canonical-form validation happens on the read side.
func EncodeRecord(k key.Key, data []byte) ([]byte, error) {
	if !payloadFits(len(data)) {
		return nil, fmt.Errorf("amberpack: object %s too large: %d bytes", k, len(data))
	}
	payload := data
	flags := byte(0)
	if comp := zstdEnc.EncodeAll(data, make([]byte, 0, len(data))); len(comp) < len(data) {
		payload = comp
		flags = flagZstd
	}
	rec := make([]byte, RecHeaderSize+len(payload))
	rec[0] = tagChunk
	copy(rec[1:33], k[:])
	rec[33] = flags
	binary.BigEndian.PutUint32(rec[34:38], uint32(len(data)))
	binary.BigEndian.PutUint32(rec[38:42], uint32(len(payload)))
	copy(rec[RecHeaderSize:], payload)
	// CRC over the whole record; the crc field itself is still zero here.
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
	return rec, nil
}

// payloadFits reports whether a payload of n bytes is within MaxPayload. Split
// out of EncodeRecord so the bound is testable without allocating it.
func payloadFits(n int) bool {
	return n <= MaxPayload
}

// ParseRecord validates the record at the start of b (which may extend past it)
// and returns its header. It checks framing, flags, key canonicality, and the
// CRC, without mutating b (b may be a read-only mmap).
func ParseRecord(b []byte) (Record, error) {
	if len(b) < RecHeaderSize {
		return Record{}, fmt.Errorf("%w: truncated record header", ErrCorrupt)
	}
	if b[0] != tagChunk {
		return Record{}, fmt.Errorf("%w: unexpected record tag %#x", ErrCorrupt, b[0])
	}
	flags := b[33]
	if flags&^flagZstd != 0 {
		return Record{}, fmt.Errorf("%w: unknown record flags %#x", ErrCorrupt, flags)
	}
	ulen := binary.BigEndian.Uint32(b[34:38])
	slen := binary.BigEndian.Uint32(b[38:42])
	if int64(len(b)) < RecHeaderSize+int64(slen) {
		return Record{}, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	if flags&flagZstd == 0 && ulen != slen {
		return Record{}, fmt.Errorf("%w: raw record with ulen %d != slen %d", ErrCorrupt, ulen, slen)
	}
	if ulen > MaxPayload {
		return Record{}, fmt.Errorf("%w: record ulen %d exceeds limit %d", ErrCorrupt, ulen, MaxPayload)
	}
	if flags&flagZstd != 0 && slen >= ulen {
		return Record{}, fmt.Errorf("%w: compressed record with slen %d >= ulen %d", ErrCorrupt, slen, ulen)
	}
	c := crc32.Update(0, castagnoli, b[:42])
	c = crc32.Update(c, castagnoli, zero4[:])
	c = crc32.Update(c, castagnoli, b[RecHeaderSize:RecHeaderSize+int(slen)])
	if c != binary.BigEndian.Uint32(b[42:46]) {
		return Record{}, fmt.Errorf("%w: record CRC mismatch", ErrCorrupt)
	}
	k, err := key.Parse(b[1:33])
	if err != nil {
		return Record{}, fmt.Errorf("%w: record key: %v", ErrCorrupt, err)
	}
	return Record{Key: k, Flags: flags, Ulen: ulen, Slen: slen}, nil
}

// DecodePayload returns caller-owned payload bytes from a record's stored
// payload. stored may be a read-only mmap slice and is never retained.
func DecodePayload(flags byte, ulen uint32, stored []byte) ([]byte, error) {
	if flags&flagZstd == 0 {
		out := make([]byte, len(stored))
		copy(out, stored)
		return out, nil
	}
	out, err := zstdDec.DecodeAll(stored, make([]byte, 0, ulen))
	if err != nil {
		return nil, fmt.Errorf("%w: zstd: %v", ErrCorrupt, err)
	}
	if uint32(len(out)) != ulen {
		return nil, fmt.Errorf("%w: decompressed to %d bytes, header says %d", ErrCorrupt, len(out), ulen)
	}
	return out, nil
}
