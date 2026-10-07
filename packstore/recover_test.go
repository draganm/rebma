package packstore

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/amberpack"
)

// activeFile writes b to a temp .seg.active file and returns the path.
func activeFile(t *testing.T, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "0000000000000001.seg.active")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// recSpan records one object's record placement inside a built body.
type recSpan struct {
	obj    Object
	off    int64
	recLen int
}

// buildBody returns header+records bytes plus each record's span.
func buildBody(t *testing.T, objs []Object) ([]byte, []recSpan) {
	t.Helper()
	body := append([]byte{}, magicHeader...)
	var spans []recSpan
	for _, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		spans = append(spans, recSpan{obj: o, off: int64(len(body)), recLen: len(rec)})
		body = append(body, rec...)
	}
	return body, spans
}

func TestScanActiveCleanFile(t *testing.T) {
	objs := testObjects(t, 5)
	body, spans := buildBody(t, objs)
	res, err := scanActive(activeFile(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if res.sealed {
		t.Fatal("clean active reported sealed")
	}
	if res.size != int64(len(body)) {
		t.Fatalf("size = %d, want %d", res.size, len(body))
	}
	if len(res.index) != len(objs) {
		t.Fatalf("index has %d keys, want %d", len(res.index), len(objs))
	}
	for _, s := range spans {
		loc, ok := res.index[s.obj.Key]
		if !ok || loc.off != s.off {
			t.Fatalf("key %s: loc %+v ok=%v want off %d", s.obj.Key, loc, ok, s.off)
		}
	}
}

func TestScanActiveTruncationAtEveryByte(t *testing.T) {
	objs := testObjects(t, 3)
	body, spans := buildBody(t, objs)
	// boundary(cut) = largest record boundary <= cut.
	boundary := func(cut int) int64 {
		b := int64(len(magicHeader))
		for _, s := range spans {
			if s.off+int64(s.recLen) <= int64(cut) {
				b = s.off + int64(s.recLen)
			}
		}
		return b
	}
	for cut := len(magicHeader); cut <= len(body); cut++ {
		res, err := scanActive(activeFile(t, body[:cut]))
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if want := boundary(cut); res.size != want {
			t.Fatalf("cut=%d: size=%d want %d", cut, res.size, want)
		}
		wantKeys := 0
		for _, s := range spans {
			if s.off+int64(s.recLen) <= res.size {
				wantKeys++
			}
		}
		if len(res.index) != wantKeys {
			t.Fatalf("cut=%d: %d keys, want %d", cut, len(res.index), wantKeys)
		}
	}
}

func TestScanActiveCorruptByteTruncatesAtThatRecord(t *testing.T) {
	objs := testObjects(t, 3)
	body, spans := buildBody(t, objs)
	last := spans[2]
	for off := last.off; off < last.off+int64(last.recLen); off++ {
		bad := bytes.Clone(body)
		bad[off] ^= 0xFF
		res, err := scanActive(activeFile(t, bad))
		if err != nil {
			t.Fatalf("off=%d: %v", off, err)
		}
		if res.size != last.off {
			t.Fatalf("corrupt byte at %d: size=%d, want truncation at %d", off, res.size, last.off)
		}
	}
}

func TestScanActiveBadHeaderResets(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("AMB"), []byte("XXXXXXXXjunkjunk")} {
		res, err := scanActive(activeFile(t, b))
		if err != nil {
			t.Fatal(err)
		}
		if res.size != 0 || len(res.index) != 0 || res.sealed {
			t.Fatalf("bad header: %+v", res)
		}
	}
}

func TestScanActiveDetectsSealedFile(t *testing.T) {
	objs := testObjects(t, 5)
	path, _ := writeSealedFile(t, objs) // a fully sealed image, named .seg
	res, err := scanActive(path)
	if err != nil {
		t.Fatal(err)
	}
	if !res.sealed {
		t.Fatal("sealed file not detected")
	}
}

func TestScanActivePartialFooterTruncates(t *testing.T) {
	objs := testObjects(t, 5)
	body, _ := buildBody(t, objs)
	bodyLen := int64(len(body))
	footerish := append(bytes.Clone(body), tagSeal)
	footerish = append(footerish, bytes.Repeat([]byte{0xAB}, 100)...) // garbage, not a valid footer
	res, err := scanActive(activeFile(t, footerish))
	if err != nil {
		t.Fatal(err)
	}
	if res.sealed {
		t.Fatal("partial footer reported sealed")
	}
	if res.size != bodyLen {
		t.Fatalf("size=%d want %d (truncate at seal marker)", res.size, bodyLen)
	}
}

func TestScanActiveMiddleRecordCorruptionNoResync(t *testing.T) {
	// The spec's core truncation semantic: the first invalid record ends the
	// valid prefix — the scan must NOT resync to later valid records.
	objs := testObjects(t, 3)
	body, spans := buildBody(t, objs)
	bad := bytes.Clone(body)
	bad[spans[1].off+5] ^= 0xFF // corrupt the middle record
	res, err := scanActive(activeFile(t, bad))
	if err != nil {
		t.Fatal(err)
	}
	if res.size != spans[1].off {
		t.Fatalf("size=%d, want truncation at %d", res.size, spans[1].off)
	}
	if len(res.index) != 1 {
		t.Fatalf("index has %d keys, want 1 (only the prefix record)", len(res.index))
	}
	if _, ok := res.index[spans[0].obj.Key]; !ok {
		t.Fatal("prefix record missing from index")
	}
}

func TestScanActiveValidFooterWithTrailingGarbage(t *testing.T) {
	// A complete valid footer followed by extra bytes is unreachable from our
	// write ordering; if encountered, the scan truncates at the seal marker
	// (un-sealing, losing nothing).
	objs := testObjects(t, 5)
	path, _ := writeSealedFile(t, objs)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tr := b[len(b)-trailerSize:]
	bodyLen := int64(binary.BigEndian.Uint64(tr[40:48]))
	withGarbage := append(bytes.Clone(b), 0xAB, 0xCD, 0xEF)
	res, err := scanActive(activeFile(t, withGarbage))
	if err != nil {
		t.Fatal(err)
	}
	if res.sealed {
		t.Fatal("file with trailing garbage reported sealed")
	}
	if res.size != bodyLen {
		t.Fatalf("size=%d, want %d (truncate at seal marker)", res.size, bodyLen)
	}
}
