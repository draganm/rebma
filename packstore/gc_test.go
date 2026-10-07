package packstore

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// gcStore returns an open store with objs sealed into segments of 8 KiB.
func gcStore(t *testing.T, objs []Object) *Store {
	t.Helper()
	dir := sealedStore(t, objs) // Put + Close seals via rotation at 8 KiB
	return openStore(t, dir)
}

func TestSegments(t *testing.T) {
	objs := testObjects(t, 24) // ~2 KiB each: several 8 KiB segments
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("no sealed segments")
	}
	var keys uint64
	for _, g := range segs {
		if g.Body <= 0 {
			t.Errorf("segment %d: Body = %d", g.ID, g.Body)
		}
		if g.Sealed.IsZero() || g.Sealed.After(time.Now()) {
			t.Errorf("segment %d: Sealed = %v", g.ID, g.Sealed)
		}
		keys += g.Keys
	}
	// Keys across sealed segments plus whatever stayed active must cover objs.
	if keys > uint64(len(objs)) {
		t.Errorf("Keys total %d > %d objects", keys, len(objs))
	}
}

func TestScanIndexCoversBody(t *testing.T) {
	s := gcStore(t, testObjects(t, 24))
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range segs {
		var sum int64
		var n uint64
		err := s.ScanIndex(g.ID, func(k key.Key, off uint64, slen uint32) {
			sum += int64(amberpack.RecHeaderSize) + int64(slen)
			n++
			if off < 8 { // records start after the header magic
				t.Errorf("segment %d: off %d inside magic", g.ID, off)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if n != g.Keys {
			t.Errorf("segment %d: scanned %d entries, Keys = %d", g.ID, n, g.Keys)
		}
		if sum != g.Body {
			t.Errorf("segment %d: Σ(46+slen) = %d, Body = %d", g.ID, sum, g.Body)
		}
	}
	if err := s.ScanIndex(99999, func(key.Key, uint64, uint32) {}); !errors.Is(err, ErrUnknownSegment) {
		t.Errorf("unknown id: err = %v, want ErrUnknownSegment", err)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	objs := testObjects(t, 24)
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range segs {
		var scanErr error
		err := s.ScanIndex(g.ID, func(k key.Key, off uint64, slen uint32) {
			raw, err := s.Record(g.ID, off)
			if err != nil {
				scanErr = err
				return
			}
			rec, err := amberpack.ParseRecord(raw)
			if err != nil {
				scanErr = err
				return
			}
			if rec.Key != k || rec.Slen != slen || len(raw) != amberpack.RecHeaderSize+int(slen) {
				t.Errorf("segment %d off %d: record mismatch", g.ID, off)
			}
		})
		if err != nil || scanErr != nil {
			t.Fatal(err, scanErr)
		}
	}
	// Bad offsets fail loudly, never panic.
	if _, err := s.Record(segs[0].ID, 0); err == nil {
		t.Error("Record at offset 0 (magic) should fail")
	}
	if _, err := s.Record(segs[0].ID, 1<<40); err == nil {
		t.Error("Record far out of range should fail")
	}
	// Offsets in the wrap window: off+RecHeaderSize overflows uint64. A
	// naive bounds check passes them and the mmap slice panics.
	for _, off := range []uint64{math.MaxUint64, math.MaxUint64 - 40} {
		if _, err := s.Record(segs[0].ID, off); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Record at wrapping offset %d: err = %v, want ErrCorrupt", off, err)
		}
	}
	// A CRC-corrupt record is reported: flip one payload byte via the mmap?
	// The mmap is read-only; instead corrupt the file on disk of a closed
	// store and reopen — covered separately in TestRecordCorrupt below.
}

func TestRecordCorrupt(t *testing.T) {
	// 8 objects: testObjects alternates highly-compressible (~78-byte record)
	// and incompressible (~2048-byte record) payloads, so 6 would stay under
	// the 8 KiB rotation threshold and never seal. 8 crosses it deterministically.
	objs := testObjects(t, 8)
	dir := sealedStore(t, objs)
	// Find the sealed file and flip a byte inside the first record's payload
	// (offset 8 is the first record header; 8+46 is its first payload byte).
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var seg string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			seg = filepath.Join(dir, e.Name())
			break
		}
	}
	if seg == "" {
		t.Fatal("no sealed segment")
	}
	f, err := os.OpenFile(seg, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte{0}
	if _, err := f.ReadAt(buf, 8+46); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, 8+46); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s := openStore(t, dir)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(segs[0].ID, 8); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Record of corrupt payload: err = %v, want ErrCorrupt", err)
	}
}

func TestHasOutside(t *testing.T) {
	objs := testObjects(t, 24)
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatal("need at least two sealed segments")
	}
	// Each key lives in exactly one segment (Put dedups), so a key found in
	// segment A is not outside A, and is outside any other segment.
	first := segs[0]
	var someKey key.Key
	if err := s.ScanIndex(first.ID, func(k key.Key, _ uint64, _ uint32) { someKey = k }); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasOutside(first.ID, someKey); err != nil || ok {
		t.Errorf("HasOutside(own segment) = %v, %v; want false, nil", ok, err)
	}
	if ok, err := s.HasOutside(segs[1].ID, someKey); err != nil || !ok {
		t.Errorf("HasOutside(other segment) = %v, %v; want true, nil", ok, err)
	}
	// After re-appending the record, the key exists in the active segment
	// too, so it is outside its original segment.
	var off uint64
	if err := s.ScanIndex(first.ID, func(k key.Key, o uint64, _ uint32) {
		if k == someKey {
			off = o
		}
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Record(first.ID, off)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendRecord(someKey, raw); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasOutside(first.ID, someKey); err != nil || !ok {
		t.Errorf("HasOutside after copy = %v, %v; want true, nil", ok, err)
	}
}

func TestAppendRecordValidates(t *testing.T) {
	// 8 objects, not 6: testObjects alternates highly-compressible
	// (~78-byte record) and incompressible (~2048-byte record) payloads,
	// so 6 would stay under the 8 KiB rotation threshold and never seal
	// (see TestRecordCorrupt); 8 crosses it deterministically.
	s := gcStore(t, testObjects(t, 8))
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	var k key.Key
	var off uint64
	if err := s.ScanIndex(segs[0].ID, func(kk key.Key, o uint64, _ uint32) { k, off = kk, o }); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Record(segs[0].ID, off)
	if err != nil {
		t.Fatal(err)
	}
	// Wrong key.
	other := blobObj(t, []byte("other-payload"))
	if err := s.AppendRecord(other.Key, raw); !errors.Is(err, ErrCorrupt) {
		t.Errorf("mismatched key: err = %v, want ErrCorrupt", err)
	}
	// Corrupt payload.
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 0xFF
	if err := s.AppendRecord(k, bad); !errors.Is(err, ErrCorrupt) {
		t.Errorf("corrupt raw: err = %v, want ErrCorrupt", err)
	}
	// Trailing junk.
	long := append(append([]byte(nil), raw...), 0)
	if err := s.AppendRecord(k, long); !errors.Is(err, ErrCorrupt) {
		t.Errorf("overlong raw: err = %v, want ErrCorrupt", err)
	}
	// Valid append round-trips through Get.
	if err := s.AppendRecord(k, raw); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(k)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.ParseRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	want, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, raw[amberpack.RecHeaderSize:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("payload mismatch after AppendRecord")
	}
}

func TestRemove(t *testing.T) {
	objs := testObjects(t, 24)
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatal("need at least two segments")
	}
	victim := segs[0]
	// Copy every record out first, like a reap does.
	type entry struct {
		k   key.Key
		off uint64
	}
	var live []entry
	if err := s.ScanIndex(victim.ID, func(k key.Key, off uint64, _ uint32) {
		live = append(live, entry{k, off})
	}); err != nil {
		t.Fatal(err)
	}
	for _, e := range live {
		raw, err := s.Record(victim.ID, e.off)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AppendRecord(e.k, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.dir, fmt.Sprintf("%016x.seg", victim.ID))
	if err := s.Remove(victim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("segment file still exists: %v", err)
	}
	// Every original object still reads back.
	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil {
			t.Fatalf("Get(%s) after Remove: %v", o.Key, err)
		}
		if !bytes.Equal(got, o.Data) {
			t.Errorf("payload mismatch for %s", o.Key)
		}
	}
	// The victim is gone from Segments and from the GC surface.
	segs2, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range segs2 {
		if g.ID == victim.ID {
			t.Error("removed segment still listed")
		}
	}
	if err := s.ScanIndex(victim.ID, func(key.Key, uint64, uint32) {}); !errors.Is(err, ErrUnknownSegment) {
		t.Errorf("ScanIndex after Remove: %v", err)
	}
	if err := s.Remove(victim.ID); !errors.Is(err, ErrUnknownSegment) {
		t.Errorf("double Remove: %v", err)
	}
	// Survives reopen: no half-state on disk.
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir)
	for _, o := range objs {
		if _, err := s2.Get(o.Key); err != nil {
			t.Fatalf("Get(%s) after reopen: %v", o.Key, err)
		}
	}
}

func TestRemoveDuringReads(t *testing.T) {
	objs := testObjects(t, 24)
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	victim := segs[0]
	var live []key.Key
	var offs []uint64
	if err := s.ScanIndex(victim.ID, func(k key.Key, off uint64, _ uint32) {
		live = append(live, k)
		offs = append(offs, off)
	}); err != nil {
		t.Fatal(err)
	}
	for i, k := range live {
		raw, err := s.Record(victim.ID, offs[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AppendRecord(k, raw); err != nil {
			t.Fatal(err)
		}
	}
	// Hammer reads of every object while the victim disappears.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			for _, o := range objs {
				if _, err := s.Get(o.Key); err != nil {
					t.Errorf("Get during Remove: %v", err)
					return
				}
			}
		}
	}()
	if err := s.Remove(victim.ID); err != nil {
		t.Fatal(err)
	}
	<-done
}

// TestRemoveWaitsForScrubs pins the victim segment via a slow ScanIndex walk
// and asserts Remove blocks until that walk finishes before it returns
// (i.e. before it would munmap). Without waitScrubs ordered strictly before
// seg.close(), this test would not fail deterministically on its own — it
// was verified by temporarily moving the wait after seg.close() and
// observing the failure (see task-4-report.md).
func TestRemoveWaitsForScrubs(t *testing.T) {
	s := gcStore(t, testObjects(t, 24))
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	victim := segs[0]
	// Buffered so the non-blocking send from inside ScanIndex's callback can
	// never be missed, however the two goroutines interleave.
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	closeRelease := sync.OnceFunc(func() { close(release) })
	defer closeRelease() // a t.Fatalf below must not leave the scan (and Close) hanging
	scanFinished := make(chan struct{})
	go func() {
		defer close(scanFinished)
		err := s.ScanIndex(victim.ID, func(key.Key, uint64, uint32) {
			select {
			case entered <- struct{}{}: // first entry only
			default:
			}
			<-release
		})
		if err != nil {
			t.Error(err)
		}
	}()
	<-entered // the scan holds a pin on the victim's mmap
	removed := make(chan error, 1)
	go func() { removed <- s.Remove(victim.ID) }()
	// Remove must not complete while the pin is held.
	select {
	case err := <-removed:
		t.Fatalf("Remove returned (%v) while a scrub held the mmap", err)
	case <-time.After(100 * time.Millisecond):
	}
	closeRelease()
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	<-scanFinished // wait out the scan goroutine before the test returns
}

func TestOldestInflightWrite(t *testing.T) {
	s := openStore(t, t.TempDir())
	if _, ok := s.OldestInflightWrite(); ok {
		t.Error("idle store reports an in-flight write")
	}
	before := time.Now()
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		seq := func(yield func(Object, error) bool) {
			o := blobObj(t, []byte("first"))
			if !yield(o, nil) {
				return
			}
			close(started)
			<-release // hold the write open
			yield(blobObj(t, []byte("second")), nil)
		}
		if err := s.WriteBatch(seq); err != nil {
			t.Error(err)
		}
	}()
	<-started
	got, ok := s.OldestInflightWrite()
	if !ok {
		t.Fatal("in-flight WriteBatch not reported")
	}
	if got.Before(before) || got.After(time.Now()) {
		t.Errorf("start %v outside [%v, now]", got, before)
	}
	close(release)
	// After the write returns, the token is gone.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := s.OldestInflightWrite(); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("write token never released")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOldestInflightWriteCoversPut(t *testing.T) {
	// Put registers even when it dedups: wrap-the-whole-call semantics are
	// only observable via the map being empty afterwards, so just check a
	// plain Put leaves no token behind and errors don't leak tokens.
	s := openStore(t, t.TempDir())
	o := blobObj(t, []byte("x"))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(o.Key, o.Data); err != nil { // dedup path
		t.Fatal(err)
	}
	if _, ok := s.OldestInflightWrite(); ok {
		t.Error("token leaked after Put")
	}
}
