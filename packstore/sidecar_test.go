package packstore

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/key"
)

func scKey(t *testing.T, i int) key.Key {
	t.Helper()
	data := []byte(fmt.Sprintf("sidecar-object-%d", i))
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func scEntry(t *testing.T, i int) sidecarRec {
	t.Helper()
	return entryRec(scKey(t, i), activeLoc{off: int64(8 + 100*i), flags: byte(i % 2), ulen: uint32(50 + i), slen: uint32(40 + i)})
}

func TestSidecarRecordRoundTrip(t *testing.T) {
	for _, want := range []sidecarRec{scEntry(t, 1), scEntry(t, 2), {kind: sidecarSynced, off: 1 << 40}} {
		enc := want.encode()
		got, ok := decodeSidecarRec(enc[:])
		if !ok || got != want {
			t.Fatalf("decode(encode(%+v)) = %+v, %v", want, got, ok)
		}
		for bit := range len(enc) * 8 {
			flipped := enc
			flipped[bit/8] ^= 1 << (bit % 8)
			if _, ok := decodeSidecarRec(flipped[:]); ok {
				t.Fatalf("kind %#x: a flip of bit %d went unnoticed", want.kind, bit)
			}
		}
	}
	e := scEntry(t, 3)
	if got := e.loc(); got != (activeLoc{off: 308, flags: 1, ulen: 53, slen: 43}) {
		t.Fatalf("loc() = %+v", got)
	}
	if got, want := e.end(), int64(308+46+43); got != want {
		t.Fatalf("end() = %d, want %d", got, want)
	}
}

func TestDecodeSidecarRecRejectsMalformedRecords(t *testing.T) {
	bad := map[string][sidecarRecSize]byte{}
	unknown := scEntry(t, 1).encode()
	unknown[0] = 0x7f
	bad["unknown kind"] = unknown
	reserved := scEntry(t, 1).encode()
	reserved[50] = 1
	bad["reserved bytes set"] = reserved
	synced := sidecarRec{kind: sidecarSynced, off: 99}.encode()
	synced[5] = 1 // a key byte: a synced record carries none
	bad["synced record with a key"] = synced
	for name, b := range bad {
		if _, ok := decodeSidecarRec(resealSidecarRec(b)); ok {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, ok := decodeSidecarRec(make([]byte, sidecarRecSize-1)); ok {
		t.Error("a short record was accepted")
	}
}

func sidecarImage(recs ...sidecarRec) []byte {
	b := bytes.Clone(sidecarMagic)
	for _, r := range recs {
		enc := r.encode()
		b = append(b, enc[:]...)
	}
	return b
}

func TestReadSidecarStopsAtFirstBadRecord(t *testing.T) {
	recs := []sidecarRec{scEntry(t, 1), {kind: sidecarSynced, off: 1000}, scEntry(t, 2)}
	img := sidecarImage(recs...)
	for cut := 0; cut <= len(img); cut++ {
		got, valid := readSidecar(img[:cut], true)
		whole, wantValid := 0, 0
		if cut >= len(sidecarMagic) {
			whole = (cut - len(sidecarMagic)) / sidecarRecSize
			wantValid = len(sidecarMagic) + whole*sidecarRecSize
		}
		if len(got) != whole || valid != wantValid {
			t.Fatalf("cut at %d: %d records, valid %d; want %d, %d", cut, len(got), valid, whole, wantValid)
		}
		for i := range got {
			if got[i] != recs[i] {
				t.Fatalf("cut at %d: record %d = %+v", cut, i, got[i])
			}
		}
	}
	// A damaged middle record hides everything after it.
	damaged := bytes.Clone(img)
	damaged[len(sidecarMagic)+sidecarRecSize+10] ^= 0xff
	got, valid := readSidecar(damaged, true)
	if len(got) != 1 || valid != len(sidecarMagic)+sidecarRecSize {
		t.Fatalf("damaged middle: %d records, valid %d", len(got), valid)
	}
	// Continuing from a record boundary needs no magic.
	rest, n := readSidecar(img[len(sidecarMagic)+sidecarRecSize:], false)
	if len(rest) != 2 || n != 2*sidecarRecSize || rest[1] != recs[2] {
		t.Fatalf("continuation: %d records, %d bytes", len(rest), n)
	}
}

func TestReadSidecarRejectsBadMagic(t *testing.T) {
	img := sidecarImage(scEntry(t, 1))
	img[0] ^= 0xff
	if got, valid := readSidecar(img, true); len(got) != 0 || valid != 0 {
		t.Fatalf("bad magic: %d records, valid %d", len(got), valid)
	}
}

func TestSidecarWriterAppendsAndResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0000000000000001.seg.active"+sidecarSuffix)
	w, err := createSidecar(path)
	if err != nil {
		t.Fatal(err)
	}
	e1, e2, e3 := scEntry(t, 1), scEntry(t, 2), scEntry(t, 3)
	w.entry(e1.k, e1.loc())
	w.synced(500)
	w.entry(e2.k, e2.loc())
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	// Resume after the second record: the third is cut off and replaced.
	w, err = openSidecarAt(path, int64(len(sidecarMagic)+2*sidecarRecSize))
	if err != nil {
		t.Fatal(err)
	}
	w.entry(e3.k, e3.loc())
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	got, _ := readSidecarFile(t, path)
	want := []sidecarRec{e1, {kind: sidecarSynced, off: 500}, e3}
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// A resume point inside the magic starts the file over.
	w, err = openSidecarAt(path, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if got, valid := readSidecarFile(t, path); len(got) != 0 || valid != len(sidecarMagic) {
		t.Fatalf("after starting over: %d records, valid %d", len(got), valid)
	}
}

func TestSidecarWriterStopsAfterAFailure(t *testing.T) {
	w, err := createSidecar(filepath.Join(t.TempDir(), "x"+sidecarSuffix))
	if err != nil {
		t.Fatal(err)
	}
	w.f.Close() // the next write fails
	e := scEntry(t, 1)
	w.entry(e.k, e.loc())
	if !w.broken {
		t.Fatal("a failed write did not mark the writer broken")
	}
	off := w.off
	w.synced(10) // must be a no-op: a later record may never follow a hole
	if w.off != off {
		t.Fatal("a broken writer kept writing")
	}
	var none *sidecarWriter // a segment without a sidecar
	none.entry(e.k, e.loc())
	none.synced(1)
	if err := none.close(); err != nil {
		t.Fatal(err)
	}
}
