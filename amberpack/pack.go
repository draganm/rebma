// Package amberpack defines the Amber-Store pack format. The content-addressed
// record codec (record.go) is shared by packstore's on-disk segments and the
// remote-sync wire packs defined here.
//
// A wire pack is a possibly-partial, unordered set of CAS objects (like a git
// pack) carrying no root key. Layout:
//
//	Magic    "AMBERPK\x03"   8 bytes  (plaintext)
//	Records  repeat: one EncodeRecord output each — a 46-byte header
//	         (tag 0x01 + key[32] + flags + ulen + slen + CRC) followed by the payload
//	End      0x00
//
// Each record is the same self-describing, CRC-protected, per-record-zstd unit
// packstore writes on disk (see record.go); a wire pack is just those records
// framed by a magic and an explicit end marker, so a truncated stream is
// detected rather than read as a clean EOF. The Reader validates framing, CRC,
// and key canonicality and decodes each payload (All), or hands the validated
// records over undecoded (Records); it does NOT verify the payload hash — that
// happens in the storage path (packstore WriteParallel with Verify).
//
// Versions 1 and 2 ("AMBERPK\x01" / "AMBERPK\x02") were the older uncompressed
// and whole-stream-zstd stream formats; they are no longer produced and are
// rejected by the Reader.
package amberpack

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/draganm/rebma/fstree"
)

// packMagic identifies the wire pack format and its version (the trailing byte).
const packMagic = "AMBERPK\x03"

// tagEnd marks the end of the record stream. A record begins with tagChunk
// (0x01, written by EncodeRecord), so the two are distinguished on the first byte.
const tagEnd byte = 0x00

// ErrMalformed wraps every error from a structurally invalid wire pack (bad or
// legacy magic, truncation, an oversized or bad record, or a corrupt record).
// Callers distinguish it with errors.Is to map to a client error.
var ErrMalformed = errors.New("amberpack: malformed pack stream")

// Writer serializes fstree.Objects into the wire pack format. It is not safe for
// concurrent use; a client wanting parallel uploads creates one Writer per pack.
type Writer struct {
	bw          *bufio.Writer
	wroteHeader bool
}

// NewWriter returns a Writer emitting to w. The caller owns w and must close it;
// Writer.Close only writes the end marker and flushes.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) ensureHeader() error {
	if w.wroteHeader {
		return nil
	}
	if _, err := w.bw.WriteString(packMagic); err != nil {
		return err
	}
	w.wroteHeader = true
	return nil
}

// Add appends one object record.
func (w *Writer) Add(o fstree.Object) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		return err
	}
	_, err = w.bw.Write(rec)
	return err
}

// AddRecord appends a pre-encoded record (an EncodeRecord output, as stored
// verbatim on disk) without decoding or re-encoding it. It is the zero-copy
// counterpart to Add: the push path reads a record straight from the local
// store and writes it to the wire, skipping the decompress/recompress round
// trip. rec is written as given; its framing and CRC are validated by the
// receiving Reader.
func (w *Writer) AddRecord(rec []byte) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	_, err := w.bw.Write(rec)
	return err
}

// Close writes the header (if no object was added) and the end marker, then
// flushes. It does not close the underlying writer.
func (w *Writer) Close() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagEnd); err != nil {
		return err
	}
	return w.bw.Flush()
}

// Reader decodes a wire pack stream.
type Reader struct {
	r io.Reader
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// RawRecord is one record of a pack as it was read: its parsed header and
// its complete bytes, header and stored payload, exactly as EncodeRecord
// produced them. Bytes is caller-owned.
type RawRecord struct {
	Record
	Bytes []byte
}

// Records iterates over the records in the stream without decoding them.
// Every record is validated exactly as All validates it (framing, the
// size bound, CRC, key canonicality) but its payload stays as stored, so
// a consumer that appends records verbatim (packstore's Object.Record,
// Writer.AddRecord) skips the decompress/recompress round trip; it is the
// read-side counterpart of AddRecord. It yields exactly one error (and
// stops) on any structural problem; on a clean stream it yields every
// record and returns after the end marker. Records must be called at
// most once per Reader because the underlying stream position is not
// reset between calls.
func (r *Reader) Records() iter.Seq2[RawRecord, error] {
	return func(yield func(RawRecord, error) bool) {
		br := bufio.NewReader(r.r)
		var magic [len(packMagic)]byte
		if _, err := io.ReadFull(br, magic[:]); err != nil {
			yield(RawRecord{}, fmt.Errorf("%w: reading magic: %v", ErrMalformed, err))
			return
		}
		if string(magic[:]) != packMagic {
			yield(RawRecord{}, fmt.Errorf("%w: bad magic", ErrMalformed))
			return
		}
		for {
			tag, err := br.ReadByte()
			if err != nil {
				yield(RawRecord{}, fmt.Errorf("%w: truncated before end marker: %v", ErrMalformed, err))
				return
			}
			switch tag {
			case tagEnd:
				return
			case tagChunk:
				// Reassemble the full record — tag + remaining 45 header bytes +
				// slen payload bytes — then validate it with ParseRecord.
				var hdr [RecHeaderSize]byte
				hdr[0] = tag
				if _, err := io.ReadFull(br, hdr[1:]); err != nil {
					yield(RawRecord{}, fmt.Errorf("%w: truncated record header: %v", ErrMalformed, err))
					return
				}
				slen := binary.BigEndian.Uint32(hdr[38:42]) // stored-payload length field
				if slen > MaxPayload {
					yield(RawRecord{}, fmt.Errorf("%w: record payload %d exceeds limit %d", ErrMalformed, slen, MaxPayload))
					return
				}
				full := make([]byte, RecHeaderSize+int(slen))
				copy(full, hdr[:])
				if _, err := io.ReadFull(br, full[RecHeaderSize:]); err != nil {
					yield(RawRecord{}, fmt.Errorf("%w: truncated record payload: %v", ErrMalformed, err))
					return
				}
				rec, err := ParseRecord(full)
				if err != nil {
					yield(RawRecord{}, fmt.Errorf("%w: %v", ErrMalformed, err))
					return
				}
				if !yield(RawRecord{Record: rec, Bytes: full}, nil) {
					return
				}
			default:
				yield(RawRecord{}, fmt.Errorf("%w: bad record tag %#x", ErrMalformed, tag))
				return
			}
		}
	}
}

// All iterates over the objects in the stream: Records with every payload
// decoded. It yields exactly one error (and stops) on any structural
// problem; on a clean stream it yields every object and returns after the
// end marker. All must be called at most once per Reader because the
// underlying stream position is not reset between calls.
func (r *Reader) All() iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		for raw, err := range r.Records() {
			if err != nil {
				yield(fstree.Object{}, err)
				return
			}
			payload, err := DecodePayload(raw.Flags, raw.Ulen, raw.Bytes[RecHeaderSize:])
			if err != nil {
				yield(fstree.Object{}, fmt.Errorf("%w: %v", ErrMalformed, err))
				return
			}
			if !yield(fstree.Object{Key: raw.Key, Bytes: payload}, nil) {
				return
			}
		}
	}
}
