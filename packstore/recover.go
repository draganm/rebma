package packstore

import (
	"bytes"
	"os"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// activeLoc locates one record inside the active segment.
type activeLoc struct {
	off   int64 // record header offset
	flags byte
	ulen  uint32
	slen  uint32
}

// scanResult is the outcome of tail-scanning an active segment file.
type scanResult struct {
	size   int64                 // valid length; the caller truncates to this (0 ⇒ reset header)
	index  map[key.Key]activeLoc // records fully contained in [0, size)
	sealed bool                  // the file carries a complete valid footer: rename it, it is sealed
}

// scanActive reads an active segment file and finds the boundary of valid
// data. Records are self-framing and CRC'd, so the scan accepts records until
// the first invalid byte and truncates there. Acknowledged (fsynced) data is
// always before that boundary: fsync covers the whole file, so a valid record
// can only be preceded by valid bytes.
func scanActive(path string) (scanResult, error) {
	res := scanResult{index: make(map[key.Key]activeLoc)}
	b, err := os.ReadFile(path)
	if err != nil {
		return res, err
	}
	if len(b) < len(magicHeader) || !bytes.Equal(b[:len(magicHeader)], magicHeader) {
		// Header never made it to disk; nothing in this file was ever
		// acknowledged (any successful fsync would have persisted the header
		// too). Reset to empty.
		return res, nil
	}
	// A complete footer survives a crash before the seal rename. Trust its
	// index even when body corruption would stop the prefix scan early.
	if _, err := parseFooter(b); err == nil {
		res.size, res.sealed = int64(len(b)), true
		return res, nil
	}
	off := int64(len(magicHeader))
	for off < int64(len(b)) {
		if b[off] == tagSeal {
			if _, err := parseFooter(b); err == nil {
				res.size, res.sealed = int64(len(b)), true
				return res, nil
			}
			// Partial footer: truncate at the seal marker. A valid footer
			// followed by trailing bytes also lands here (parseFooter anchors
			// the trailer at EOF); that state cannot arise from our write
			// ordering, and truncating it only un-seals, never loses records.
			break
		}
		rec, err := amberpack.ParseRecord(b[off:])
		if err != nil {
			break // invalid or truncated: everything from off on is garbage
		}
		res.index[rec.Key] = activeLoc{off: off, flags: rec.Flags, ulen: rec.Ulen, slen: rec.Slen}
		off += amberpack.RecHeaderSize + int64(rec.Slen)
	}
	res.size = off
	return res, nil
}
