// Package packstore persists Amber-Store CAS objects in log-structured,
// append-only segment (pack) files. Sealed segments are immutable, mmap'd
// whole, and self-indexed by a footer (fanout index on the first key byte +
// binary fuse filter + fixed trailer). An active segment is indexed in its
// owner's memory and, for everybody else and for the next open, by a sidecar
// file beside it (sidecar.go). There is no global index. A directory may be
// open in any number of stores, in any number of processes: a writer owns an
// active segment of its own (active.go), readers lock nothing and look at
// the directory again when they miss (view.go), and one lock file keeps
// writers and a GC sweep apart (gate.go). All format integers are big-endian.
// Record framing lives in the amberpack package. See
// architecture/packstore.md.
package packstore

import (
	"hash/crc32"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

const (
	tagSeal   byte = 0xF0 // first byte of the footer
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
)

var (
	magicHeader  = []byte("AMBERSG\x01")
	magicTrailer = []byte("AMBERSGF")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli) // footer CRC; record CRC lives in amberpack
)

// ErrCorrupt wraps every structural-corruption error (bad record framing, bad
// footer, scrub findings). It aliases amberpack's record-corruption sentinel so
// a single errors.Is target covers both record- and footer-level corruption.
var ErrCorrupt = amberpack.ErrCorrupt

// Object is one CAS object to store: its key and either its serialized
// bytes (Data) or, for an object that was encoded elsewhere, the complete
// record as amberpack.EncodeRecord produced it (Record). Exactly one of
// the two is set. A Record is parsed (framing, CRC, canonical key, key
// equal to Key) and appended verbatim, so a caller that already holds
// encoded records, say a pack it staged on disk, skips the compression
// round trip; with WriteOpts.Verify its payload is decoded and rehashed
// like Data is.
type Object struct {
	Key    key.Key
	Data   []byte
	Record []byte
}
