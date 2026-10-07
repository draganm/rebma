package packstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"slices"
	"sort"

	"github.com/FastFilter/xorfilter"
	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
	"golang.org/x/sys/unix"
)

const (
	fanoutSize     = 256 * 4 // 256 cumulative u32 counts on the key's first byte
	indexEntrySize = 32 + 8 + 4
	trailerSize    = 64
)

// indexEntry is one sealed-segment index row: a key and where its record
// starts, plus the stored payload length (authoritative for reads;
// footer-CRC-protected and cross-checked against the body by scrub).
type indexEntry struct {
	k    key.Key
	off  uint64 // file offset of the record header
	slen uint32
}

// compareEntries orders by full key. The fanout is on the first byte, so its
// buckets are runs of this order: a key leads with its hash, which is
// uniformly distributed, while the last byte is type/length-size and clusters.
func compareEntries(a, b indexEntry) int {
	return bytes.Compare(a.k[:], b.k[:])
}

// buildIndexSection serializes the index section (fanout + sorted entries).
// It does not mutate entries. Callers must pass entries with distinct keys
// (the write path dedups; with duplicate keys the relative order of their
// rows is unspecified).
func buildIndexSection(entries []indexEntry) []byte {
	es := slices.Clone(entries)
	slices.SortFunc(es, compareEntries)

	out := make([]byte, fanoutSize+len(es)*indexEntrySize)
	var counts [256]uint32
	for _, e := range es {
		counts[e.k[0]]++
	}
	cum := uint32(0)
	for b := 0; b < 256; b++ {
		cum += counts[b]
		binary.BigEndian.PutUint32(out[b*4:], cum)
	}
	off := fanoutSize
	for _, e := range es {
		copy(out[off:off+32], e.k[:])
		binary.BigEndian.PutUint64(out[off+32:], e.off)
		binary.BigEndian.PutUint32(out[off+40:], e.slen)
		off += indexEntrySize
	}
	return out
}

// parseIndexSection splits an index section into a decoded fanout table and
// the raw entry bytes, validating lengths and fanout monotonicity.
func parseIndexSection(b []byte, keyCount uint64) (*[256]uint32, []byte, error) {
	if keyCount > math.MaxUint32 {
		return nil, nil, fmt.Errorf("%w: key count %d exceeds format limit", ErrCorrupt, keyCount)
	}
	want := uint64(fanoutSize) + keyCount*indexEntrySize
	if uint64(len(b)) != want {
		return nil, nil, fmt.Errorf("%w: index section is %d bytes, want %d", ErrCorrupt, len(b), want)
	}
	var fanout [256]uint32
	prev := uint32(0)
	for i := 0; i < 256; i++ {
		fanout[i] = binary.BigEndian.Uint32(b[i*4:])
		if fanout[i] < prev {
			return nil, nil, fmt.Errorf("%w: fanout not monotonic at byte %#x", ErrCorrupt, i)
		}
		prev = fanout[i]
	}
	if uint64(fanout[255]) != keyCount {
		return nil, nil, fmt.Errorf("%w: fanout total %d != key count %d", ErrCorrupt, fanout[255], keyCount)
	}
	return &fanout, b[fanoutSize:], nil
}

const (
	filterHeaderSize       = 29
	filterTypeBinaryFuse16 = 1
)

// filterKey is the filter input for k: the first 8 bytes of the key, which lie
// in the uniformly distributed truncated-hash region.
func filterKey(k key.Key) uint64 {
	return binary.BigEndian.Uint64(k[:8])
}

// buildFilterSection builds and serializes a binary fuse filter over the
// entries' keys. Duplicate 8-byte heads are deduplicated before the build.
func buildFilterSection(entries []indexEntry) ([]byte, error) {
	heads := make([]uint64, 0, len(entries))
	for _, e := range entries {
		heads = append(heads, filterKey(e.k))
	}
	slices.Sort(heads)
	heads = slices.Compact(heads)
	f, err := xorfilter.NewBinaryFuse[uint16](heads)
	if err != nil {
		return nil, fmt.Errorf("packstore: building fuse filter: %w", err)
	}
	out := make([]byte, filterHeaderSize+2*len(f.Fingerprints))
	out[0] = filterTypeBinaryFuse16
	binary.BigEndian.PutUint64(out[1:9], f.Seed)
	binary.BigEndian.PutUint32(out[9:13], f.SegmentLength)
	binary.BigEndian.PutUint32(out[13:17], f.SegmentLengthMask)
	binary.BigEndian.PutUint32(out[17:21], f.SegmentCount)
	binary.BigEndian.PutUint32(out[21:25], f.SegmentCountLength)
	binary.BigEndian.PutUint32(out[25:29], uint32(len(f.Fingerprints)))
	for i, fp := range f.Fingerprints {
		binary.BigEndian.PutUint16(out[filterHeaderSize+2*i:], fp)
	}
	return out, nil
}

// parseFilterSection deserializes a filter section, copying fingerprints out
// of b (which may be a read-only mmap) into RAM. The five geometry fields are
// validated against the binary-fuse construction invariants: Contains indexes
// Fingerprints from them, so crafted values would otherwise panic the read
// path rather than fail parse with ErrCorrupt.
func parseFilterSection(b []byte) (*xorfilter.BinaryFuse[uint16], error) {
	if len(b) < filterHeaderSize {
		return nil, fmt.Errorf("%w: filter section too short: %d bytes", ErrCorrupt, len(b))
	}
	if b[0] != filterTypeBinaryFuse16 {
		return nil, fmt.Errorf("%w: unknown filter type %d", ErrCorrupt, b[0])
	}
	fpCount := binary.BigEndian.Uint32(b[25:29])
	if uint64(len(b)) != filterHeaderSize+2*uint64(fpCount) {
		return nil, fmt.Errorf("%w: filter section is %d bytes, want %d", ErrCorrupt, len(b), filterHeaderSize+2*uint64(fpCount))
	}
	segLen := binary.BigEndian.Uint32(b[9:13])
	segLenMask := binary.BigEndian.Uint32(b[13:17])
	segCount := binary.BigEndian.Uint32(b[17:21])
	segCountLen := binary.BigEndian.Uint32(b[21:25])
	switch {
	case segCount == 0:
		return nil, fmt.Errorf("%w: filter segment count is zero", ErrCorrupt)
	case segLen == 0 || segLen&(segLen-1) != 0,
		segLenMask != segLen-1,
		uint64(segCountLen) != uint64(segCount)*uint64(segLen),
		uint64(fpCount) != uint64(segCountLen)+2*uint64(segLen):
		return nil, fmt.Errorf("%w: filter geometry invalid", ErrCorrupt)
	}
	f := &xorfilter.BinaryFuse[uint16]{
		Seed:               binary.BigEndian.Uint64(b[1:9]),
		SegmentLength:      segLen,
		SegmentLengthMask:  segLenMask,
		SegmentCount:       segCount,
		SegmentCountLength: segCountLen,
		Fingerprints:       make([]uint16, fpCount),
	}
	for i := range f.Fingerprints {
		f.Fingerprints[i] = binary.BigEndian.Uint16(b[filterHeaderSize+2*i:])
	}
	return f, nil
}

// searchIndex finds k in a parsed index section: fanout bucket on the first
// byte, then binary search on the full key within the bucket.
func searchIndex(fanout *[256]uint32, entries []byte, k key.Key) (off uint64, slen uint32, ok bool) {
	pos, ok := searchIndexPos(fanout, entries, k)
	if !ok {
		return 0, 0, false
	}
	e := entries[pos*indexEntrySize:]
	return binary.BigEndian.Uint64(e[32:40]), binary.BigEndian.Uint32(e[40:44]), true
}

// searchIndexPos returns k's entry position within the index section.
func searchIndexPos(fanout *[256]uint32, entries []byte, k key.Key) (pos int, ok bool) {
	b := k[0]
	lo := uint32(0)
	if b > 0 {
		lo = fanout[b-1]
	}
	n := int(fanout[b] - lo)
	i := sort.Search(n, func(i int) bool {
		e := entries[(int(lo)+i)*indexEntrySize:]
		return bytes.Compare(e[:32], k[:]) >= 0
	})
	if i >= n {
		return 0, false
	}
	pos = int(lo) + i
	if !bytes.Equal(entries[pos*indexEntrySize:][:32], k[:]) {
		return 0, false
	}
	return pos, true
}

// buildFooter assembles the complete footer (seal marker, index section,
// filter section, trailer) for a segment whose records end at bodyLen.
func buildFooter(bodyLen int64, entries []indexEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("packstore: refusing to seal an empty segment")
	}
	idx := buildIndexSection(entries)
	filt, err := buildFilterSection(entries)
	if err != nil {
		return nil, err
	}
	footer := make([]byte, 0, 1+len(idx)+len(filt)+trailerSize)
	footer = append(footer, tagSeal)
	footer = append(footer, idx...)
	footer = append(footer, filt...)

	tr := make([]byte, trailerSize)
	indexOff := uint64(bodyLen) + 1
	binary.BigEndian.PutUint64(tr[0:8], indexOff)
	binary.BigEndian.PutUint64(tr[8:16], uint64(len(idx)))
	binary.BigEndian.PutUint64(tr[16:24], indexOff+uint64(len(idx)))
	binary.BigEndian.PutUint64(tr[24:32], uint64(len(filt)))
	binary.BigEndian.PutUint64(tr[32:40], uint64(len(entries)))
	binary.BigEndian.PutUint64(tr[40:48], uint64(bodyLen))
	copy(tr[56:64], magicTrailer)
	footer = append(footer, tr...)
	// footerCRC covers [bodyLen, EOF-16): everything up to and excluding the
	// crc field itself; reserved and magic are checked explicitly on parse.
	binary.BigEndian.PutUint32(footer[len(footer)-16:], crc32.Checksum(footer[:len(footer)-16], castagnoli))
	return footer, nil
}

// footerView is the parsed footer of a sealed segment. fanout and filter live
// in RAM; entries points into the segment's mmap.
type footerView struct {
	fanout   [256]uint32
	entries  []byte
	filter   *xorfilter.BinaryFuse[uint16]
	keyCount uint64
	bodyLen  int64
	indexOff int64
	indexLen int64
}

// parseFooter validates a whole sealed-segment image (header through trailer)
// and returns its footer view. mm may be a read-only mmap; nothing is mutated.
func parseFooter(mm []byte) (*footerView, error) {
	if len(mm) < len(magicHeader)+1+fanoutSize+indexEntrySize+filterHeaderSize+trailerSize {
		return nil, fmt.Errorf("%w: file too short: %d bytes", ErrCorrupt, len(mm))
	}
	if !bytes.Equal(mm[:len(magicHeader)], magicHeader) {
		return nil, fmt.Errorf("%w: bad header magic", ErrCorrupt)
	}
	tr := mm[len(mm)-trailerSize:]
	if !bytes.Equal(tr[56:64], magicTrailer) {
		return nil, fmt.Errorf("%w: bad trailer magic", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(tr[52:56]) != 0 {
		return nil, fmt.Errorf("%w: nonzero reserved trailer field", ErrCorrupt)
	}
	indexOff := binary.BigEndian.Uint64(tr[0:8])
	indexLen := binary.BigEndian.Uint64(tr[8:16])
	filterOff := binary.BigEndian.Uint64(tr[16:24])
	filterLen := binary.BigEndian.Uint64(tr[24:32])
	keyCount := binary.BigEndian.Uint64(tr[32:40])
	bodyLen := binary.BigEndian.Uint64(tr[40:48])

	fileLen := uint64(len(mm))
	switch {
	case bodyLen < uint64(len(magicHeader)) || bodyLen >= fileLen,
		keyCount > math.MaxUint32, // fanout counts are u32; also keeps the next line overflow-free
		indexOff != bodyLen+1,
		indexLen != uint64(fanoutSize)+keyCount*uint64(indexEntrySize),
		filterOff != indexOff+indexLen,
		filterOff > fileLen-trailerSize,
		filterLen != fileLen-trailerSize-filterOff:
		return nil, fmt.Errorf("%w: trailer offsets inconsistent", ErrCorrupt)
	}
	if crc32.Checksum(mm[bodyLen:fileLen-16], castagnoli) != binary.BigEndian.Uint32(tr[48:52]) {
		return nil, fmt.Errorf("%w: footer CRC mismatch", ErrCorrupt)
	}
	if mm[bodyLen] != tagSeal {
		return nil, fmt.Errorf("%w: missing seal marker", ErrCorrupt)
	}

	fv := &footerView{
		keyCount: keyCount,
		bodyLen:  int64(bodyLen),
		indexOff: int64(indexOff),
		indexLen: int64(indexLen),
	}
	fanout, entries, err := parseIndexSection(mm[indexOff:indexOff+indexLen], keyCount)
	if err != nil {
		return nil, err
	}
	fv.fanout = *fanout
	fv.entries = entries
	if fv.filter, err = parseFilterSection(mm[filterOff : filterOff+filterLen]); err != nil {
		return nil, err
	}
	return fv, nil
}

// lookup finds k in the segment's index.
func (fv *footerView) lookup(k key.Key) (off uint64, slen uint32, ok bool) {
	return searchIndex(&fv.fanout, fv.entries, k)
}

// lookupPos finds k's position in the segment's index — the mark-set bit
// slot for the record (see markset.go).
func (fv *footerView) lookupPos(k key.Key) (pos int, ok bool) {
	return searchIndexPos(&fv.fanout, fv.entries, k)
}

// sealedSegment is an immutable, fully mmap'd segment.
type sealedSegment struct {
	id   uint64
	path string
	fi   os.FileInfo // which file this is: a repair replaces a segment under its name, and an id can come back
	mm   []byte
	fv   *footerView
}

// openSealed maps a sealed segment and validates its footer. The fd is closed
// after mapping; the mapping keeps the file content alive.
func openSealed(path string, id uint64) (*sealedSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(len(magicHeader)+1+fanoutSize+indexEntrySize+filterHeaderSize+trailerSize) {
		return nil, fmt.Errorf("%w: %s: file too short: %d bytes", ErrCorrupt, path, st.Size())
	}
	mm, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("packstore: mmap %s: %w", path, err)
	}
	fv, err := parseFooter(mm)
	if err != nil {
		unix.Munmap(mm)
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &sealedSegment{id: id, path: path, fi: st, mm: mm, fv: fv}, nil
}

func (g *sealedSegment) close() error {
	return unix.Munmap(g.mm)
}

// has reports whether k is in this segment: fuse filter first (cheap,
// probabilistic), then the exact index.
func (g *sealedSegment) has(k key.Key) bool {
	if !g.fv.filter.Contains(filterKey(k)) {
		return false
	}
	_, _, ok := g.fv.lookup(k)
	return ok
}

// get returns k's payload (caller-owned), whether it was found, and any
// corruption error. The hot path does not CRC-check; that is scrub's job.
func (g *sealedSegment) get(k key.Key) ([]byte, bool, error) {
	if !g.fv.filter.Contains(filterKey(k)) {
		return nil, false, nil
	}
	off, slen, ok := g.fv.lookup(k)
	if !ok {
		return nil, false, nil
	}
	bodyLen := uint64(g.fv.bodyLen)
	if off < uint64(len(magicHeader)) || off > bodyLen ||
		uint64(amberpack.RecHeaderSize)+uint64(slen) > bodyLen-off {
		return nil, false, fmt.Errorf("%w: %s: index entry out of bounds", ErrCorrupt, g.path)
	}
	end := off + amberpack.RecHeaderSize + uint64(slen)
	h := g.mm[off : off+amberpack.RecHeaderSize]
	flags := h[33]
	ulen := binary.BigEndian.Uint32(h[34:38])
	data, err := amberpack.DecodePayload(flags, ulen, g.mm[off+amberpack.RecHeaderSize:end])
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", g.path, err)
	}
	return data, true, nil
}

// getRecord returns a caller-owned copy of k's full on-disk record (header +
// stored payload, undecoded), whether it was found, and any corruption error.
// Like get, the hot path does not CRC-check; the record is validated by the
// receiving Reader on the push path.
func (g *sealedSegment) getRecord(k key.Key) ([]byte, bool, error) {
	if !g.fv.filter.Contains(filterKey(k)) {
		return nil, false, nil
	}
	off, slen, ok := g.fv.lookup(k)
	if !ok {
		return nil, false, nil
	}
	bodyLen := uint64(g.fv.bodyLen)
	if off < uint64(len(magicHeader)) || off > bodyLen ||
		uint64(amberpack.RecHeaderSize)+uint64(slen) > bodyLen-off {
		return nil, false, fmt.Errorf("%w: %s: index entry out of bounds", ErrCorrupt, g.path)
	}
	end := off + amberpack.RecHeaderSize + uint64(slen)
	rec := make([]byte, end-off)
	copy(rec, g.mm[off:end])
	return rec, true, nil
}

// storedSize returns k's stored (post-compression) payload length and whether
// it was found, from the index alone — no payload read.
func (g *sealedSegment) storedSize(k key.Key) (uint32, bool) {
	if !g.fv.filter.Contains(filterKey(k)) {
		return 0, false
	}
	_, slen, ok := g.fv.lookup(k)
	return slen, ok
}

// locate returns k's record offset within this segment and whether it was
// found, from the index alone — for ordering reads by disk layout.
func (g *sealedSegment) locate(k key.Key) (uint64, bool) {
	if !g.fv.filter.Contains(filterKey(k)) {
		return 0, false
	}
	off, _, ok := g.fv.lookup(k)
	return off, ok
}
