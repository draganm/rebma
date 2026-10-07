package amberpack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/draganm/rebma/key"
)

// incompressible returns n deterministic pseudo-random bytes (zstd cannot shrink them).
func incompressible(n int) []byte {
	r := rand.New(rand.NewPCG(42, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// compressible returns n highly repetitive bytes (zstd shrinks them a lot).
func compressible(n int) []byte {
	return bytes.Repeat([]byte("abcdefgh"), n/8+1)[:n]
}

// r0ulen reads the ulen field of a record.
func r0ulen(rec []byte) uint32 { return binary.BigEndian.Uint32(rec[34:38]) }

// fixCRC recomputes a record's CRC after test tampering.
func fixCRC(rec []byte) {
	binary.BigEndian.PutUint32(rec[42:46], 0)
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
}

func TestRecordRoundTripRaw(t *testing.T) {
	o := mkObj(t, incompressible(4096))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Key != o.Key {
		t.Fatalf("key mismatch: %s != %s", r.Key, o.Key)
	}
	if r.Flags != 0 {
		t.Fatalf("random data must be stored raw, got flags %#x", r.Flags)
	}
	if r.Ulen != r.Slen || int(r.Slen) != len(o.Bytes) {
		t.Fatalf("raw lens: ulen=%d slen=%d want %d", r.Ulen, r.Slen, len(o.Bytes))
	}
	got, err := DecodePayload(r.Flags, r.Ulen, rec[RecHeaderSize:RecHeaderSize+int(r.Slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, o.Bytes) {
		t.Fatal("payload mismatch")
	}
}

func TestRecordRoundTripCompressed(t *testing.T) {
	o := mkObj(t, compressible(64<<10))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Flags != flagZstd {
		t.Fatalf("repetitive data must compress, got flags %#x", r.Flags)
	}
	if r.Slen >= r.Ulen {
		t.Fatalf("compressed slen=%d must be < ulen=%d", r.Slen, r.Ulen)
	}
	got, err := DecodePayload(r.Flags, r.Ulen, rec[RecHeaderSize:RecHeaderSize+int(r.Slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, o.Bytes) {
		t.Fatal("payload mismatch after decompression")
	}
}

func TestRecordEmptyPayload(t *testing.T) {
	o := mkObj(t, nil)
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Ulen != 0 || r.Slen != 0 || r.Flags != 0 {
		t.Fatalf("empty payload: ulen=%d slen=%d flags=%#x", r.Ulen, r.Slen, r.Flags)
	}
}

func TestRecordTooLarge(t *testing.T) {
	if !payloadFits(MaxPayload) {
		t.Fatal("payloadFits must accept exactly MaxPayload")
	}
	if payloadFits(MaxPayload + 1) {
		t.Fatal("payloadFits must reject MaxPayload+1")
	}
	if !payloadFits(0) {
		t.Fatal("payloadFits must accept empty payloads")
	}
}

func TestParseRecordRejectsOversizedUlen(t *testing.T) {
	o := mkObj(t, compressible(4096))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if rec[33]&flagZstd == 0 {
		t.Fatal("test needs a compressed record")
	}
	binary.BigEndian.PutUint32(rec[34:38], math.MaxUint32)
	fixCRC(rec)
	if _, err := ParseRecord(rec); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestParseRecordRejectsCorruption(t *testing.T) {
	o := mkObj(t, incompressible(1024))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated header", func(t *testing.T) {
		if _, err := ParseRecord(rec[:RecHeaderSize-1]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("truncated payload", func(t *testing.T) {
		if _, err := ParseRecord(rec[:len(rec)-1]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("bad tag", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[0] = 0x7F
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("bad flags", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[33] = 0x80
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("flipped payload byte fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[len(bad)-1] ^= 0x01
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("flipped length fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[39] ^= 0x01 // inside slen, keeps record long enough to parse
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("non-canonical key", func(t *testing.T) {
		// Encode with an invalid type nibble; EncodeRecord does not validate
		// keys (callers supply canonical keys), ParseRecord must.
		var k key.Key
		copy(k[:], o.Key[:])
		k[31] = 0xF0 // type 15: reserved
		bad, err := EncodeRecord(k, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("raw ulen != slen", func(t *testing.T) {
		bad := bytes.Clone(rec)
		binary.BigEndian.PutUint32(bad[34:38], r0ulen(bad)+1)
		fixCRC(bad)
		if _, err := ParseRecord(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
}

// TestParseRecordIgnoresTrailingBytes checks the invariant that ParseRecord is
// called on mmap slices and tail-scan buffers that extend past the current
// record; trailing bytes must not affect the parse result or the CRC.
func TestParseRecordIgnoresTrailingBytes(t *testing.T) {
	o := mkObj(t, incompressible(512))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	extended := append(bytes.Clone(rec), incompressible(1000)...)
	r, err := ParseRecord(extended)
	if err != nil {
		t.Fatal(err)
	}
	if r.Key != o.Key || int(r.Slen) != len(o.Bytes) {
		t.Fatalf("parse over extended buffer: %+v", r)
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	t.Run("bad zstd frame", func(t *testing.T) {
		if _, err := DecodePayload(flagZstd, 100, []byte("not a zstd frame")); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
	t.Run("bomb stops at ulen", func(t *testing.T) {
		bomb := zstdEnc.EncodeAll(make([]byte, 64<<20), nil)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := DecodePayload(flagZstd, 1024, bomb)
		runtime.ReadMemStats(&after)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
			t.Fatalf("decoding allocated %d bytes for a 1 KiB ulen", grew)
		}
	})
	t.Run("ulen mismatch", func(t *testing.T) {
		comp := zstdEnc.EncodeAll([]byte("hello world"), nil)
		if _, err := DecodePayload(flagZstd, 5, comp); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
}

func TestDecodePayloadRawDoesNotAlias(t *testing.T) {
	stored := []byte{1, 2, 3, 4}
	out, err := DecodePayload(0, 4, stored)
	if err != nil {
		t.Fatal(err)
	}
	out[0] = 99
	if stored[0] != 1 {
		t.Fatal("DecodePayload raw path must copy, not alias")
	}
}
