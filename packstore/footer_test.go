package packstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// testEntries builds n index entries with distinct keys and synthetic offsets.
func testEntries(t *testing.T, n int) []indexEntry {
	t.Helper()
	entries := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		data := append(incompressible(64), byte(i), byte(i>>8), byte(i>>16))
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{k: k, off: uint64(8 + i*100), slen: uint32(i + 1)})
	}
	return entries
}

func TestIndexSectionLookup(t *testing.T) {
	entries := testEntries(t, 1000)
	idx := buildIndexSection(entries)
	if len(idx) != fanoutSize+len(entries)*indexEntrySize {
		t.Fatalf("index length %d", len(idx))
	}
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok {
			t.Fatalf("key %s not found", e.k)
		}
		if off != e.off || slen != e.slen {
			t.Fatalf("key %s: got (%d,%d) want (%d,%d)", e.k, off, slen, e.off, e.slen)
		}
	}
}

func TestIndexSectionAbsentKey(t *testing.T) {
	entries := testEntries(t, 100)
	idx := buildIndexSection(entries)
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	absent := blobObj(t, []byte("definitely not stored")).Key
	if _, _, ok := searchIndex(fanout, entryBytes, absent); ok {
		t.Fatal("absent key reported present")
	}
}

func TestIndexSectionSingleEntryAndEdgeBuckets(t *testing.T) {
	// Force first bytes 0x00 and 0xFF to cover the b==0 lower bound and the
	// final bucket.
	for _, first := range []byte{0x00, 0xFF, 0x80} {
		e := testEntries(t, 1)[0]
		e.k[0] = first
		idx := buildIndexSection([]indexEntry{e})
		fanout, entryBytes, err := parseIndexSection(idx, 1)
		if err != nil {
			t.Fatal(err)
		}
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok || off != e.off || slen != e.slen {
			t.Fatalf("first=%#x: ok=%v off=%d slen=%d", first, ok, off, slen)
		}
		miss := e.k
		miss[30] ^= 0xFF
		if _, _, ok := searchIndex(fanout, entryBytes, miss); ok {
			t.Fatalf("first=%#x: absent key found", first)
		}
	}
}

func TestIndexSectionDoesNotMutateInput(t *testing.T) {
	entries := testEntries(t, 50)
	orig := slices.Clone(entries)
	_ = buildIndexSection(entries)
	if !slices.Equal(entries, orig) {
		t.Fatal("buildIndexSection mutated its input")
	}
}

func TestParseIndexSectionRejectsCorruption(t *testing.T) {
	entries := testEntries(t, 10)
	idx := buildIndexSection(entries)

	t.Run("wrong length", func(t *testing.T) {
		if _, _, err := parseIndexSection(idx[:len(idx)-1], 10); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("non-monotonic fanout", func(t *testing.T) {
		bad := slices.Clone(idx)
		// Make fanout[1] < fanout[0] by forcing fanout[0] huge.
		binary.BigEndian.PutUint32(bad[0:4], 0xFFFFFFFF)
		if _, _, err := parseIndexSection(bad, 10); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("fanout total mismatch", func(t *testing.T) {
		// Keep the section length correct for keyCount=1 but zero the whole
		// fanout: monotonic, total 0 != 1 — must hit the total check, not the
		// length check.
		one := buildIndexSection(testEntries(t, 1))
		bad := slices.Clone(one)
		for i := 0; i < fanoutSize; i++ {
			bad[i] = 0
		}
		_, _, err := parseIndexSection(bad, 1)
		if err == nil {
			t.Fatal("want error")
		}
		if !strings.Contains(err.Error(), "fanout total") {
			t.Fatalf("wrong branch: %v", err)
		}
	})
	t.Run("huge keyCount does not wrap", func(t *testing.T) {
		// (2^64-984)/44 made the old int arithmetic compute want==40, so a
		// 40-byte section passed the length check and the fanout loop
		// panicked. The fixed code must reject it as corrupt instead.
		huge := (math.MaxUint64 - uint64(983)) / indexEntrySize
		_, _, err := parseIndexSection(idx[:40], huge)
		if err == nil {
			t.Fatal("want error, not a panic")
		}
		if !strings.Contains(err.Error(), "exceeds format limit") {
			t.Fatalf("wrong branch: %v", err)
		}
	})
}

func TestIndexSectionEmptyAndEmptyBucket(t *testing.T) {
	// Empty section round-trips and misses cleanly.
	idx := buildIndexSection(nil)
	fanout, entryBytes, err := parseIndexSection(idx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := searchIndex(fanout, entryBytes, blobObj(t, []byte("x")).Key); ok {
		t.Fatal("found key in empty index")
	}

	// Deterministic empty-bucket miss: one entry with first byte 0x10,
	// search a key with first byte 0x20 (a guaranteed-empty bucket).
	e := testEntries(t, 1)[0]
	e.k[0] = 0x10
	idx = buildIndexSection([]indexEntry{e})
	fanout, entryBytes, err = parseIndexSection(idx, 1)
	if err != nil {
		t.Fatal(err)
	}
	probe := e.k
	probe[0] = 0x20
	if _, _, ok := searchIndex(fanout, entryBytes, probe); ok {
		t.Fatal("found key in empty bucket")
	}
}

func TestFilterSectionMembership(t *testing.T) {
	entries := testEntries(t, 5000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !f.Contains(filterKey(e.k)) {
			t.Fatalf("false negative for %s", e.k)
		}
	}
}

func TestFilterSectionFalsePositiveRate(t *testing.T) {
	entries := testEntries(t, 1000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	// 16-bit fingerprints: FP rate ~2^-16. Expect ~1.5 hits in 100k probes;
	// 50 leaves astronomical margin while still catching a broken filter.
	fp := 0
	for i := uint64(0); i < 100_000; i++ {
		if f.Contains(0xDEAD_0000_0000_0000 + i) {
			fp++
		}
	}
	if fp > 50 {
		t.Fatalf("false positive rate too high: %d/100000", fp)
	}
}

func TestFilterSectionDuplicateHeads(t *testing.T) {
	// Two entries with an identical 8-byte head must not break the build.
	entries := testEntries(t, 2)
	copy(entries[1].k[:8], entries[0].k[:8])
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Contains(filterKey(entries[0].k)) || !f.Contains(filterKey(entries[1].k)) {
		t.Fatal("false negative on duplicate heads")
	}
}

func TestParseFilterSectionRejectsCorruption(t *testing.T) {
	sec, err := buildFilterSection(testEntries(t, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("short", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:10]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad type", func(t *testing.T) {
		bad := slices.Clone(sec)
		bad[0] = 99
		if _, err := parseFilterSection(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("length mismatch", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:len(sec)-2]); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestIndexSectionGoldenBytes(t *testing.T) {
	// Pin the on-disk encoding against symmetric encode/decode bugs: two
	// fixed entries, exact expected bytes.
	var k1, k2 key.Key
	k1[31] = 0x00 // Blob, 1-byte length field
	k1[30] = 0x05 // length 5
	k1[0] = 0x02
	k2[31] = 0x00
	k2[30] = 0x07
	k2[0] = 0x01 // sorts before k1 (first byte)
	idx := buildIndexSection([]indexEntry{
		{k: k1, off: 0x1122334455667788, slen: 0xAABBCCDD},
		{k: k2, off: 8, slen: 1},
	})

	// fanout: bytes 0x00 → 0, 0x01 → 1, 0x02..0xFF → 2 (cumulative, BE)
	if got := binary.BigEndian.Uint32(idx[0:4]); got != 0 {
		t.Fatalf("fanout[0] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[4:8]); got != 1 {
		t.Fatalf("fanout[1] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[8:12]); got != 2 {
		t.Fatalf("fanout[2] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[1020:1024]); got != 2 {
		t.Fatalf("fanout[255] = %d", got)
	}

	// First entry must be k2 (first byte 0x01): key bytes, then off/slen BE.
	e0 := idx[fanoutSize : fanoutSize+indexEntrySize]
	if !bytes.Equal(e0[:32], k2[:]) {
		t.Fatalf("entry 0 key = %x", e0[:32])
	}
	if !bytes.Equal(e0[32:40], []byte{0, 0, 0, 0, 0, 0, 0, 8}) {
		t.Fatalf("entry 0 off bytes = %x", e0[32:40])
	}
	if !bytes.Equal(e0[40:44], []byte{0, 0, 0, 1}) {
		t.Fatalf("entry 0 slen bytes = %x", e0[40:44])
	}

	e1 := idx[fanoutSize+indexEntrySize : fanoutSize+2*indexEntrySize]
	if !bytes.Equal(e1[:32], k1[:]) {
		t.Fatalf("entry 1 key = %x", e1[:32])
	}
	if !bytes.Equal(e1[32:40], []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}) {
		t.Fatalf("entry 1 off bytes = %x", e1[32:40])
	}
	if !bytes.Equal(e1[40:44], []byte{0xAA, 0xBB, 0xCC, 0xDD}) {
		t.Fatalf("entry 1 slen bytes = %x", e1[40:44])
	}
}

func TestParseFilterSectionRejectsBadGeometry(t *testing.T) {
	sec, err := buildFilterSection(testEntries(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(name string, f func(b []byte)) {
		t.Run(name, func(t *testing.T) {
			bad := slices.Clone(sec)
			f(bad)
			_, err := parseFilterSection(bad)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("want ErrCorrupt, got %v", err)
			}
		})
	}
	// Crafted geometry previously made xorfilter's Contains panic with
	// index-out-of-range; parse must reject it instead.
	mutate("segCountLen inflated", func(b []byte) {
		binary.BigEndian.PutUint32(b[21:25], 0xFFFFFFF0)
	})
	mutate("mask inflated", func(b []byte) {
		binary.BigEndian.PutUint32(b[13:17], 0xFFFFFFFF)
	})
	mutate("segLen not power of two", func(b []byte) {
		binary.BigEndian.PutUint32(b[9:13], binary.BigEndian.Uint32(b[9:13])+1)
	})
	mutate("segLen zero", func(b []byte) {
		binary.BigEndian.PutUint32(b[9:13], 0)
		binary.BigEndian.PutUint32(b[13:17], 0xFFFFFFFF)
	})
	// segCount == 0 (with segCountLen == 0 and fpCount == 2*segLen) satisfies
	// every other geometry identity, so it needs its own case. This crafted
	// geometry previously made xorfilter's Contains panic with
	// index-out-of-range on first lookup; parse must reject it instead.
	t.Run("segCount zero", func(t *testing.T) {
		segLen := binary.BigEndian.Uint32(sec[9:13])
		fpCount := 2 * segLen
		bad := slices.Clone(sec[:filterHeaderSize+2*int(fpCount)])
		binary.BigEndian.PutUint32(bad[17:21], 0) // segCount
		binary.BigEndian.PutUint32(bad[21:25], 0) // segCountLen
		binary.BigEndian.PutUint32(bad[25:29], fpCount)
		_, err := parseFilterSection(bad)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("want ErrCorrupt, got %v", err)
		}
	})
}

func TestFilterSectionFieldRoundTrip(t *testing.T) {
	entries := testEntries(t, 1234)
	heads := make([]uint64, 0, len(entries))
	for _, e := range entries {
		heads = append(heads, filterKey(e.k))
	}
	slices.Sort(heads)
	heads = slices.Compact(heads)
	want, err := xorfilter.NewBinaryFuse[uint16](heads)
	if err != nil {
		t.Fatal(err)
	}
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seed != want.Seed ||
		got.SegmentLength != want.SegmentLength ||
		got.SegmentLengthMask != want.SegmentLengthMask ||
		got.SegmentCount != want.SegmentCount ||
		got.SegmentCountLength != want.SegmentCountLength ||
		!slices.Equal(got.Fingerprints, want.Fingerprints) {
		t.Fatalf("field round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// writeSealedFile assembles a complete sealed segment on disk from objects:
// header, records, footer. Returns the path and the entries written.
func writeSealedFile(t *testing.T, objs []Object) (string, []indexEntry) {
	t.Helper()
	var body []byte
	body = append(body, magicHeader...)
	var entries []indexEntry
	for _, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{
			k: o.Key, off: uint64(len(body)),
			slen: uint32(len(rec) - amberpack.RecHeaderSize),
		})
		body = append(body, rec...)
	}
	footer, err := buildFooter(int64(len(body)), entries)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "0000000000000001.seg")
	if err := os.WriteFile(path, append(body, footer...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path, entries
}

func testObjects(t *testing.T, n int) []Object {
	t.Helper()
	objs := make([]Object, 0, n)
	for i := 0; i < n; i++ {
		var data []byte
		if i%2 == 0 {
			data = append(compressible(2000), byte(i), byte(i>>8))
		} else {
			data = append(incompressible(2000), byte(i), byte(i>>8))
		}
		objs = append(objs, blobObj(t, data))
	}
	return objs
}

func TestSealedSegmentRoundTrip(t *testing.T) {
	objs := testObjects(t, 200)
	path, _ := writeSealedFile(t, objs)
	seg, err := openSealed(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()
	if seg.fv.keyCount != 200 {
		t.Fatalf("keyCount = %d", seg.fv.keyCount)
	}
	for _, o := range objs {
		if !seg.has(o.Key) {
			t.Fatalf("has(%s) = false", o.Key)
		}
		data, found, err := seg.get(o.Key)
		if err != nil || !found {
			t.Fatalf("get(%s): found=%v err=%v", o.Key, found, err)
		}
		if !bytes.Equal(data, o.Data) {
			t.Fatalf("get(%s): payload mismatch", o.Key)
		}
	}
	absent := blobObj(t, []byte("not here")).Key
	if seg.has(absent) {
		t.Fatal("has(absent) = true")
	}
	if _, found, _ := seg.get(absent); found {
		t.Fatal("get(absent) found")
	}
}

func TestOpenSealedRejectsCorruption(t *testing.T) {
	objs := testObjects(t, 20)
	path, _ := writeSealedFile(t, objs)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	corrupt := func(t *testing.T, mutate func(b []byte)) {
		t.Helper()
		b := bytes.Clone(good)
		mutate(b)
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openSealed(p, 1); err == nil {
			t.Fatal("want error")
		}
	}

	t.Run("trailer magic", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[len(b)-1] ^= 0xFF })
	})
	t.Run("footer CRC over index", func(t *testing.T) {
		corrupt(t, func(b []byte) {
			tr := b[len(b)-trailerSize:]
			indexOff := binary.BigEndian.Uint64(tr[0:8])
			b[indexOff+10] ^= 0xFF
		})
	})
	t.Run("header magic", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[0] ^= 0xFF })
	})
	t.Run("reserved nonzero", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[len(b)-12] = 1 }) // CRC does not cover the last 16 bytes; reserved is checked explicitly
	})
	t.Run("truncated", func(t *testing.T) {
		b := bytes.Clone(good[:len(good)-100])
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openSealed(p, 1); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestBuildFooterRejectsEmpty(t *testing.T) {
	if _, err := buildFooter(8, nil); err == nil {
		t.Fatal("want error for empty segment")
	}
}

// refreshFooterCRC recomputes a doctored file's footer CRC so parseFooter's
// CRC check passes and deeper validation is exercised.
func refreshFooterCRC(b []byte) {
	tr := b[len(b)-trailerSize:]
	bodyLen := binary.BigEndian.Uint64(tr[40:48])
	binary.BigEndian.PutUint32(tr[48:52], crc32.Checksum(b[bodyLen:uint64(len(b))-16], castagnoli))
}

func TestParseFooterRejectsWrappingTrailer(t *testing.T) {
	objs := testObjects(t, 20)
	path, _ := writeSealedFile(t, objs)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// keyCount=MaxUint32 forces indexLen ~190 GB, so filterOff >> fileLen;
	// a filterLen chosen to wrap mod 2^64 made the old sum check pass and
	// the index slice expression panic.
	tr := b[len(b)-trailerSize:]
	indexOff := binary.BigEndian.Uint64(tr[0:8])
	keyCount := uint64(math.MaxUint32)
	indexLen := uint64(fanoutSize) + keyCount*indexEntrySize
	filterOff := indexOff + indexLen
	fileLen := uint64(len(b))
	binary.BigEndian.PutUint64(tr[8:16], indexLen)
	binary.BigEndian.PutUint64(tr[16:24], filterOff)
	binary.BigEndian.PutUint64(tr[24:32], fileLen-trailerSize-filterOff) // wraps
	binary.BigEndian.PutUint64(tr[32:40], keyCount)
	refreshFooterCRC(b)
	p := filepath.Join(t.TempDir(), "0000000000000001.seg")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openSealed(p, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestSealedGetRejectsCraftedOffsets(t *testing.T) {
	objs := testObjects(t, 8)
	for _, badOff := range []uint64{math.MaxInt64, 1 << 62} {
		path, _ := writeSealedFile(t, objs)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// Rewrite index entry 0's off field, fix the CRC, reopen, and Get the
		// entry's own key: the bounds check must answer ErrCorrupt, not panic.
		tr := b[len(b)-trailerSize:]
		indexOff := binary.BigEndian.Uint64(tr[0:8])
		entryPos := indexOff + fanoutSize
		var k key.Key
		copy(k[:], b[entryPos:entryPos+32])
		binary.BigEndian.PutUint64(b[entryPos+32:entryPos+40], badOff)
		refreshFooterCRC(b)
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		seg, err := openSealed(p, 1)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = seg.get(k)
		seg.close()
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("off=%#x: want ErrCorrupt, got %v", badOff, err)
		}
	}
}

func TestOpenSealedTinyFiles(t *testing.T) {
	for _, n := range []int{0, trailerSize} {
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openSealed(p, 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("size %d: want ErrCorrupt, got %v", n, err)
		}
	}
}
