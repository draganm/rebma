package packstore

import (
	"bytes"
	"cmp"
	"io"
	"os"
	"slices"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// segmentScan is how far an active segment has been read: its index, the end
// of the valid data indexed so far, how much of the sidecar was consumed, and
// the data length known durable. An owner builds one at adoption; a reader
// keeps one per foreign segment and takes it forward as the owner appends.
type segmentScan struct {
	index      map[key.Key]activeLoc
	pos        int64
	sidecarEnd int64
	durable    int64
}

// recovered is what an active segment holds, worked out from its sidecar and
// its data file. Working it out never writes; the segment's owner applies it
// (truncating the data, bringing the sidecar in line), a reader just uses the
// index.
type recovered struct {
	index      map[key.Key]activeLoc
	dataEnd    int64        // end of the valid data; short of the header: it never became durable
	sidecarEnd int64        // leading sidecar bytes that agree with the data; 0: start the sidecar over
	durable    int64        // data length the sidecar knows durable
	missing    []sidecarRec // valid records the agreeing part of the sidecar does not list, in data order
	sealed     bool         // the data file carries a complete footer: a seal's rename is outstanding
	fileSize   int64        // the data file's length when it was looked at
}

func (r recovered) scan() *segmentScan {
	return &segmentScan{index: r.index, pos: r.dataEnd, sidecarEnd: r.sidecarEnd, durable: r.durable}
}

// recoverSegment recovers the active segment at path by the sidecar's
// reading rules (sidecar.go), falling back to a full scan of the data when
// the sidecar cannot be used.
func recoverSegment(path string) (recovered, error) {
	// The sidecar first, the data's length after: a live owner writes a
	// record before its entry, so the data read later covers whatever the
	// sidecar read earlier speaks of. An unreadable sidecar is a missing
	// one: the data file is the truth.
	sidecar, _ := os.ReadFile(path + sidecarSuffix)
	f, err := os.Open(path)
	if err != nil {
		return recovered{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return recovered{}, err
	}
	res, ok, err := recoverFrom(f, st.Size(), sidecar)
	if err != nil {
		return recovered{}, err
	}
	if ok {
		res.fileSize = st.Size()
		return res, nil
	}
	res, err = fullScan(path)
	res.fileSize = st.Size()
	return res, err
}

// fullScan is recovery without a sidecar: every record is read and checked.
func fullScan(path string) (recovered, error) {
	res, err := scanActive(path)
	if err != nil {
		return recovered{}, err
	}
	out := recovered{index: res.index, dataEnd: res.size, durable: int64(len(magicHeader)), sealed: res.sealed}
	for k, loc := range res.index {
		out.missing = append(out.missing, entryRec(k, loc))
	}
	slices.SortFunc(out.missing, func(a, b sidecarRec) int { return cmp.Compare(a.off, b.off) })
	return out, nil
}

// recoverFrom applies the reading rules to a data file of the given size and
// the bytes of its sidecar. ok is false when the sidecar cannot be relied on
// at all, or the file looks like a crashed seal: the caller then scans it
// whole.
func recoverFrom(data io.ReaderAt, size int64, sidecar []byte) (res recovered, ok bool, err error) {
	headerLen := int64(len(magicHeader))
	if _, valid := readSidecar(sidecar, true); valid == 0 || size < headerLen {
		return recovered{}, false, nil
	}
	header, err := readRange(data, 0, headerLen)
	if err != nil {
		return recovered{}, false, err
	}
	if !bytes.Equal(header, magicHeader) {
		return recovered{}, false, nil
	}
	if size >= headerLen+trailerSize {
		tail, err := readRange(data, size-int64(len(magicTrailer)), int64(len(magicTrailer)))
		if err != nil {
			return recovered{}, false, err
		}
		if bytes.Equal(tail, magicTrailer) {
			return recovered{}, false, nil // a footer: the full scan decides whether it is whole
		}
	}
	sc := &segmentScan{index: make(map[key.Key]activeLoc), pos: headerLen, durable: headerLen}
	added, missing, ok, err := sc.advance(data, size, sidecar)
	if err != nil || !ok {
		return recovered{}, false, err
	}
	for _, r := range added {
		sc.index[r.k] = r.loc()
	}
	return recovered{index: sc.index, dataEnd: sc.pos, sidecarEnd: sc.sidecarEnd, durable: sc.durable, missing: missing}, true, nil
}

// advance takes the scan forward over what was appended since it last ran:
// tail holds the sidecar's bytes from sc.sidecarEnd on, size is the data
// file's length, read after the sidecar was. It returns the index entries to
// add — the caller applies them, so that a shared view can be extended under
// its own lock — and, among those, the ones the sidecar does not list. ok is
// false when sidecar and data contradict what the scan already holds: the
// caller starts over.
//
// Entries whose record ends within the data known durable (the largest synced
// record) are trusted unread. Later entries are verified against the data, in
// order, until one fails. What follows the last good entry is scanned record
// by record, which finds records written — perhaps acknowledged — just before
// a crash kept their entries from the sidecar.
func (sc *segmentScan) advance(data io.ReaderAt, size int64, tail []byte) (added, missing []sidecarRec, ok bool, err error) {
	headerLen := int64(len(magicHeader))
	if sc.pos < headerLen { // a view of a segment whose header had not arrived yet
		if size < headerLen {
			return nil, nil, true, nil
		}
		header, err := readRange(data, 0, headerLen)
		if err != nil {
			return nil, nil, false, err
		}
		if !bytes.Equal(header, magicHeader) {
			return nil, nil, true, nil
		}
		sc.pos = headerLen
	}

	atStart := sc.sidecarEnd == 0
	recs, valid := readSidecar(tail, atStart)
	consumed := int64(0)
	if atStart && valid > 0 {
		consumed = int64(len(sidecarMagic))
	}
	durable := sc.durable
	for _, r := range recs {
		if r.kind != sidecarSynced {
			continue
		}
		if int64(r.off) > size || int64(r.off) < 0 {
			// More durable data than the file holds: whatever happened to
			// the file, this sidecar does not describe it.
			return nil, nil, false, nil
		}
		durable = max(durable, int64(r.off))
	}
entries:
	for _, r := range recs {
		if r.kind == sidecarEntry {
			switch {
			case int64(r.off) < sc.pos:
				// Indexed already, by an earlier scan of the data's tail: the
				// entry arrived after the record. Anything else is a
				// contradiction.
				if cur, has := sc.index[r.k]; !has || cur != r.loc() {
					return nil, nil, false, nil
				}
			case int64(r.off) == sc.pos && r.end() <= size && r.end() > sc.pos:
				if r.end() > durable {
					good, err := verifyEntry(data, r)
					if err != nil {
						return nil, nil, false, err
					}
					if !good {
						break entries
					}
				}
				added = append(added, r)
				sc.pos = r.end()
			default:
				// Records are contiguous and each has its entry, so one that
				// does not start where the last ended, or runs past the
				// file, ends what the sidecar can vouch for.
				break entries
			}
		}
		consumed += sidecarRecSize
	}
	sc.sidecarEnd += consumed
	sc.durable = durable

	if sc.pos < size {
		rest, err := readRange(data, sc.pos, size-sc.pos)
		if err != nil {
			return nil, nil, false, err
		}
		off := 0
		for off < len(rest) && rest[off] != tagSeal { // a seal marker without a whole footer: a torn seal
			rec, err := amberpack.ParseRecord(rest[off:])
			if err != nil {
				break // invalid, truncated or still being written: nothing past here counts yet
			}
			r := entryRec(rec.Key, activeLoc{off: sc.pos + int64(off), flags: rec.Flags, ulen: rec.Ulen, slen: rec.Slen})
			added = append(added, r)
			missing = append(missing, r)
			off += amberpack.RecHeaderSize + int(rec.Slen)
		}
		sc.pos += int64(off)
	}
	return added, missing, true, nil
}

// verifyEntry reads the record an entry points at and checks that it is
// whole and is the record the entry describes.
func verifyEntry(data io.ReaderAt, r sidecarRec) (bool, error) {
	raw, err := readRange(data, int64(r.off), r.end()-int64(r.off))
	if err != nil {
		return false, err
	}
	rec, err := amberpack.ParseRecord(raw)
	return err == nil && rec.Key == r.k && rec.Flags == r.flags && rec.Ulen == r.ulen && rec.Slen == r.slen, nil
}

func readRange(data io.ReaderAt, off, n int64) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(data, off, n), b); err != nil {
		return nil, err
	}
	return b, nil
}

// openOwnedSidecar brings a recovered segment's sidecar in line for its new
// owner: it keeps the part that agrees with the data, drops the rest and
// lists the records that were missing. It never fails the open. A sidecar
// that cannot be written must not stay as it is — recovery would go on
// trusting it — so it is removed, and the segment runs without one.
func openOwnedSidecar(dataPath string, res recovered) *sidecarWriter {
	path := dataPath + sidecarSuffix
	w, err := openSidecarAt(path, res.sidecarEnd)
	if err != nil {
		os.Remove(path)
		return nil
	}
	for _, r := range res.missing {
		w.write(r)
	}
	return w
}
