package packstore

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// An active segment's index lives in its owner's memory. The sidecar,
// <id>.seg.active.idx, mirrors it on disk as an append-only log, so that
// opening the segment — by the next owner or by any reader — does not have to
// parse the data file: an 8-byte magic, then fixed-size big-endian records,
//
//	offset  size  field
//	0       1     kind   sidecarEntry or sidecarSynced
//	1       32    key    entry: the record's key            synced: zero
//	33      8     off    entry: offset of the record header synced: data length known durable
//	41      1     flags  entry: the record's flags byte     synced: zero
//	42      4     ulen   entry: uncompressed payload length synced: zero
//	46      4     slen   entry: stored payload length       synced: zero
//	50      2     zero   reserved
//	52      4     crc    CRC-32C of bytes [0:52]
//
// The owner appends an entry only after the record's write to the data file
// returned, and a synced record only after an fsync of the data file
// returned. The sidecar itself is never fsynced: it is a cache, and recovery
// (recover.go) trusts entries up to the last synced record, verifies the rest
// against the data, and scans what the sidecar does not cover. See
// architecture/packstore.md.
const (
	sidecarSuffix  = ".idx" // appended to the active segment's file name
	sidecarRecSize = 56

	sidecarEntry  byte = 0x01
	sidecarSynced byte = 0x02
)

var sidecarMagic = []byte("AMBERIX\x01")

type sidecarRec struct {
	kind  byte
	k     key.Key
	off   uint64
	flags byte
	ulen  uint32
	slen  uint32
}

func entryRec(k key.Key, loc activeLoc) sidecarRec {
	return sidecarRec{kind: sidecarEntry, k: k, off: uint64(loc.off), flags: loc.flags, ulen: loc.ulen, slen: loc.slen}
}

// loc is an entry's place in the data file.
func (r sidecarRec) loc() activeLoc {
	return activeLoc{off: int64(r.off), flags: r.flags, ulen: r.ulen, slen: r.slen}
}

// end is the data offset just past an entry's record.
func (r sidecarRec) end() int64 {
	return int64(r.off) + amberpack.RecHeaderSize + int64(r.slen)
}

func (r sidecarRec) encode() [sidecarRecSize]byte {
	var b [sidecarRecSize]byte
	b[0] = r.kind
	copy(b[1:33], r.k[:])
	binary.BigEndian.PutUint64(b[33:41], r.off)
	b[41] = r.flags
	binary.BigEndian.PutUint32(b[42:46], r.ulen)
	binary.BigEndian.PutUint32(b[46:50], r.slen)
	binary.BigEndian.PutUint32(b[52:56], crc32.Checksum(b[:52], castagnoli))
	return b
}

// decodeSidecarRec parses one record, reporting false for anything the
// encoder would not have produced.
func decodeSidecarRec(b []byte) (sidecarRec, bool) {
	if len(b) < sidecarRecSize {
		return sidecarRec{}, false
	}
	if crc32.Checksum(b[:52], castagnoli) != binary.BigEndian.Uint32(b[52:56]) {
		return sidecarRec{}, false
	}
	r := sidecarRec{
		kind:  b[0],
		off:   binary.BigEndian.Uint64(b[33:41]),
		flags: b[41],
		ulen:  binary.BigEndian.Uint32(b[42:46]),
		slen:  binary.BigEndian.Uint32(b[46:50]),
	}
	copy(r.k[:], b[1:33])
	if b[50] != 0 || b[51] != 0 {
		return sidecarRec{}, false
	}
	switch r.kind {
	case sidecarEntry:
	case sidecarSynced:
		if r.k != (key.Key{}) || r.flags != 0 || r.ulen != 0 || r.slen != 0 {
			return sidecarRec{}, false
		}
	default:
		return sidecarRec{}, false
	}
	return r, true
}

// readSidecar parses b: a whole sidecar file (atStart), or the bytes from a
// record boundary on. It returns the records up to the first invalid or
// partial one and the number of bytes of b they cover, magic included. A bad
// magic yields nothing.
func readSidecar(b []byte, atStart bool) (recs []sidecarRec, valid int) {
	off := 0
	if atStart {
		if len(b) < len(sidecarMagic) || !bytes.Equal(b[:len(sidecarMagic)], sidecarMagic) {
			return nil, 0
		}
		off = len(sidecarMagic)
	}
	for off+sidecarRecSize <= len(b) {
		r, ok := decodeSidecarRec(b[off : off+sidecarRecSize])
		if !ok {
			break
		}
		recs = append(recs, r)
		off += sidecarRecSize
	}
	return recs, off
}

// sidecarWriter appends to a sidecar. A failed write never fails the store's
// write — the data is intact, and recovery copes with a short index — but it
// stops the writer for good, so that no record ever follows a hole. A nil
// writer is a segment without a sidecar; every method accepts it.
type sidecarWriter struct {
	f      *os.File
	off    int64
	broken bool
}

// createSidecar starts path over: magic only.
func createSidecar(path string) (*sidecarWriter, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.WriteAt(sidecarMagic, 0); err != nil {
		f.Close()
		return nil, err
	}
	return &sidecarWriter{f: f, off: int64(len(sidecarMagic))}, nil
}

// openSidecarAt keeps the first valid bytes of path — what recovery found to
// agree with the data — drops the rest and appends after them. A valid length
// short of the magic starts the file over.
func openSidecarAt(path string, valid int64) (*sidecarWriter, error) {
	if valid < int64(len(sidecarMagic)) {
		return createSidecar(path)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, err
	}
	return &sidecarWriter{f: f, off: valid}, nil
}

func (w *sidecarWriter) write(r sidecarRec) {
	if w == nil || w.broken {
		return
	}
	b := r.encode()
	if _, err := w.f.WriteAt(b[:], w.off); err != nil {
		w.broken = true
		return
	}
	w.off += sidecarRecSize
}

// entry records that the record for k was written at loc.
func (w *sidecarWriter) entry(k key.Key, loc activeLoc) { w.write(entryRec(k, loc)) }

// synced records that an fsync made the first dataLen bytes of the data file durable.
func (w *sidecarWriter) synced(dataLen int64) {
	w.write(sidecarRec{kind: sidecarSynced, off: uint64(dataLen)})
}

func (w *sidecarWriter) close() error {
	if w == nil {
		return nil
	}
	return w.f.Close()
}
