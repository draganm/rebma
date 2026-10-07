package fstree

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
)

// lookupEntryDecoded is LookupEntry as it was before the scan: the full
// decoder on every object on the way down. The scan must not change a result,
// an error or the text of an error, so the tests hold LookupEntry to it.
func lookupEntryDecoded(dir key.Key, name []byte, get func(key.Key) ([]byte, error)) (Entry, error) {
	k, err := DirOf(dir, get)
	if err != nil {
		return Entry{}, err
	}
	for {
		data, err := get(k)
		if err != nil {
			return Entry{}, fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		switch k.Type() {
		case key.DirLeaf:
			entries, err := DecodeDirLeaf(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
			}
			i := sort.Search(len(entries), func(i int) bool {
				return bytes.Compare(entries[i].Name, name) >= 0
			})
			if i < len(entries) && bytes.Equal(entries[i].Name, name) {
				return entries[i], nil
			}
			return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
		case key.DirNode:
			pairs, err := DecodeDirNode(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
			}
			i := sort.Search(len(pairs), func(i int) bool {
				return bytes.Compare(pairs[i].SepName, name) >= 0
			})
			if i == len(pairs) {
				return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
			}
			ck, err := key.Parse(pairs[i].ChildKey)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			k = ck
		default:
			return Entry{}, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
		}
	}
}

// errChain lists err and everything it wraps, each with its type and text.
func errChain(err error) []string {
	var out []string
	for ; err != nil; err = errors.Unwrap(err) {
		out = append(out, fmt.Sprintf("%T: %v", err, err))
	}
	return out
}

// sameLookup fails the test unless LookupEntry and the decoder-only path give
// the same entry, or the same chain of errors with the same text. A mutated
// tree may point back at itself, so each walk gets a bounded number of reads.
func sameLookup(t testing.TB, dir key.Key, name []byte, get func(key.Key) ([]byte, error)) {
	t.Helper()
	bounded := func() func(key.Key) ([]byte, error) {
		reads := 0
		return func(k key.Key) ([]byte, error) {
			if reads++; reads > 64 {
				return nil, errors.New("too many reads")
			}
			return get(k)
		}
	}
	got, gotErr := LookupEntry(dir, name, bounded())
	want, wantErr := lookupEntryDecoded(dir, name, bounded())
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LookupEntry(%s, %q) = %+v, the decoder alone gives %+v", dir, name, got, want)
	}
	if g, w := errChain(gotErr), errChain(wantErr); !reflect.DeepEqual(g, w) {
		t.Fatalf("LookupEntry(%s, %q) fails with %q, the decoder alone with %q", dir, name, g, w)
	}
}

// agreesLeaf reports false when the scan leaves name in b to the decoder.
// When it answers, the decoder, whose result for b is (dec, decErr), must
// have taken b and must give the same answer.
func agreesLeaf(t testing.TB, b, name []byte, dec []Entry, decErr error) bool {
	t.Helper()
	enc, res := scanLeaf(b, name)
	if res == scanUnsure {
		return false
	}
	if decErr != nil {
		t.Fatalf("scanLeaf(%x, %q) = %d for a body the decoder refuses: %v", b, name, res, decErr)
	}
	i := sort.Search(len(dec), func(i int) bool { return bytes.Compare(dec[i].Name, name) >= 0 })
	present := i < len(dec) && bytes.Equal(dec[i].Name, name)
	switch res {
	case scanFound:
		// The entry alone, through the decoder, as LookupEntry takes it.
		e, eres := scanLeafEntry(b, name)
		if eres != scanFound {
			t.Fatalf("scanLeaf(%x, %q): entry %x does not decode", b, name, enc)
		}
		if !present || !reflect.DeepEqual(e, dec[i]) {
			t.Fatalf("scanLeaf(%x, %q) found %+v, the decoder gives %+v (present: %v)", b, name, e, dec, present)
		}
	case scanMissing:
		if present {
			t.Fatalf("scanLeaf(%x, %q) misses what the decoder finds: %+v", b, name, dec[i])
		}
	}
	return true
}

// agreesNode is agreesLeaf for a DirNode body.
func agreesNode(t testing.TB, b, name []byte, dec []DirPair, decErr error) bool {
	t.Helper()
	child, res := scanNode(b, name)
	if res == scanUnsure {
		return false
	}
	if decErr != nil {
		t.Fatalf("scanNode(%x, %q) = %d for a body the decoder refuses: %v", b, name, res, decErr)
	}
	i := sort.Search(len(dec), func(i int) bool { return bytes.Compare(dec[i].SepName, name) >= 0 })
	switch res {
	case scanFound:
		if i == len(dec) || !bytes.Equal(child, dec[i].ChildKey) {
			t.Fatalf("scanNode(%x, %q) = child %x, the decoder gives pair %d of %+v", b, name, child, i, dec)
		}
	case scanMissing:
		if i != len(dec) {
			t.Fatalf("scanNode(%x, %q) misses what the decoder finds: %+v", b, name, dec[i])
		}
	}
	return true
}

func randBytes(r *rand.Rand, n int, alphabet string) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.IntN(len(alphabet))]
	}
	return b
}

// randNames returns up to n distinct names in order.
func randNames(r *rand.Rand, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = randBytes(r, 1+r.IntN(12), "abcxyz._-/\x00\xff")
	}
	slices.SortFunc(out, bytes.Compare)
	return slices.CompactFunc(out, bytes.Equal)
}

// randBig returns an integer of any encoded width.
func randBig(r *rand.Rand) uint64 {
	switch r.IntN(4) {
	case 0:
		return r.Uint64N(24)
	case 1:
		return r.Uint64N(1 << 16)
	case 2:
		return r.Uint64N(1 << 40)
	default:
		return r.Uint64()
	}
}

// randEntry returns an entry with any of the optional keys the scan reads.
func randEntry(r *rand.Rand, name []byte) Entry {
	e := Entry{
		Name:  name,
		Mode:  []uint64{0o100644, 0o100755, 0o040755, 0o120777, 0o020644}[r.IntN(5)],
		UID:   randBig(r),
		GID:   randBig(r),
		Mtime: int64(r.Uint64()),
	}
	if r.IntN(4) != 0 {
		e.ContentKey = randBytes(r, 32, "\x00\x01\x02\xff")
	}
	if r.IntN(4) == 0 {
		e.LinkTarget = randBytes(r, 1+r.IntN(40), "ab/.")
	}
	if r.IntN(8) == 0 {
		e.Rdev = []uint64{randBig(r), randBig(r)}
	}
	if r.IntN(8) == 0 {
		e.XattrsKey = randBytes(r, 32, "\x07\x08")
	}
	return e
}

// mutations returns damaged copies of b: single bits flipped, truncations,
// a byte appended, and runs of bytes cut out or repeated.
func mutations(r *rand.Rand, b []byte) [][]byte {
	var out [][]byte
	for range 24 {
		m := bytes.Clone(b)
		if len(m) > 0 {
			m[r.IntN(len(m))] ^= 1 << r.IntN(8)
		}
		out = append(out, m)
	}
	for range 4 {
		out = append(out, bytes.Clone(b[:r.IntN(len(b)+1)]))
	}
	out = append(out, append(bytes.Clone(b), 0))
	for range 4 {
		i := r.IntN(len(b) + 1)
		j := i + r.IntN(len(b)-i+1)
		out = append(out, slices.Concat(b[:i], b[j:]))
		out = append(out, slices.Concat(b[:j], b[i:]))
	}
	return out
}

// mutantQueries is how many of a directory's names each damaged copy of its
// body is asked for. More bodies find more than more names per body, and the
// tests stay quick under the race detector.
const mutantQueries = 8

func marshal(t testing.TB, v any) []byte {
	t.Helper()
	b, err := encMode.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestScanLeafAgreesWithDecoder(t *testing.T) {
	r := rand.New(rand.NewPCG(0x9e3779b97f4a7c15, 0))
	var answered, deferredDecodable, deferredRefused int
	for range 500 {
		names := randNames(r, r.IntN(40))
		entries := make([]Entry, len(names))
		for i, name := range names {
			entries[i] = randEntry(r, name)
		}
		b := marshal(t, entries)
		queries := append(slices.Clone(names), randNames(r, 4)...)
		queries = append(queries, []byte{})
		dec, decErr := DecodeDirLeaf(b)
		for _, q := range queries {
			if !agreesLeaf(t, b, q, dec, decErr) {
				t.Fatalf("canonical leaf %x left to the decoder for %q", b, q)
			}
		}
		for _, m := range mutations(r, b) {
			dec, decErr := DecodeDirLeaf(m)
			for range mutantQueries {
				q := queries[r.IntN(len(queries))]
				switch {
				case agreesLeaf(t, m, q, dec, decErr):
					answered++
				case decErr == nil:
					deferredDecodable++
				default:
					deferredRefused++
				}
			}
		}
	}
	t.Logf("mutated leaves: scan answered %d lookups, left %d to a decoder that takes the body and %d to one that refuses it",
		answered, deferredDecodable, deferredRefused)
	if answered == 0 || deferredDecodable == 0 || deferredRefused == 0 {
		t.Fatal("the mutations do not reach every outcome")
	}
}

func TestScanNodeAgreesWithDecoder(t *testing.T) {
	r := rand.New(rand.NewPCG(0x2545f4914f6cdd1d, 0))
	var answered, deferredDecodable, deferredRefused int
	for range 500 {
		names := randNames(r, r.IntN(40))
		pairs := make([]DirPair, len(names))
		for i, name := range names {
			pairs[i] = DirPair{SepName: name, ChildKey: randBytes(r, 32, "\x03\x04\x05")}
		}
		b := marshal(t, pairs)
		queries := append(slices.Clone(names), randNames(r, 4)...)
		queries = append(queries, []byte{})
		dec, decErr := DecodeDirNode(b)
		for _, q := range queries {
			if !agreesNode(t, b, q, dec, decErr) {
				t.Fatalf("canonical node %x left to the decoder for %q", b, q)
			}
		}
		for _, m := range mutations(r, b) {
			dec, decErr := DecodeDirNode(m)
			for range mutantQueries {
				q := queries[r.IntN(len(queries))]
				switch {
				case agreesNode(t, m, q, dec, decErr):
					answered++
				case decErr == nil:
					deferredDecodable++
				default:
					deferredRefused++
				}
			}
		}
	}
	t.Logf("mutated nodes: scan answered %d lookups, left %d to a decoder that takes the body and %d to one that refuses it",
		answered, deferredDecodable, deferredRefused)
	if answered == 0 || deferredDecodable == 0 || deferredRefused == 0 {
		t.Fatal("the mutations do not reach every outcome")
	}
}

func TestScanLeafDefersInlineXattrsAndUnsortedNames(t *testing.T) {
	a := Entry{Name: []byte("a"), XattrsIn: cbor.RawMessage{0xa1, 0x41, 'k', 0x41, 'v'}}
	b := marshal(t, []Entry{a})
	if _, res := scanLeaf(b, []byte("a")); res != scanUnsure {
		t.Errorf("scanLeaf of an entry with inline xattrs = %d, want scanUnsure", res)
	}

	b = marshal(t, []Entry{{Name: []byte("z")}, {Name: []byte("y")}})
	if _, res := scanLeaf(b, []byte("y")); res != scanUnsure {
		t.Errorf("scanLeaf of unsorted names = %d, want scanUnsure", res)
	}
}

// testTree builds a directory of two levels from the entries, leaves of a few
// entries under one DirNode, and returns its objects in the order built, the
// root last.
func testTree(t testing.TB, r *rand.Rand, entries []Entry) (keys []key.Key, store map[key.Key][]byte) {
	t.Helper()
	store = map[key.Key][]byte{}
	put := func(typ key.Type, body []byte) key.Key {
		k, err := key.New(typ, uint64(len(body)), body)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		store[k] = body
		return k
	}
	var pairs []DirPair
	for len(entries) > 0 {
		run := min(len(entries), 1+r.IntN(8))
		leaf := put(key.DirLeaf, marshal(t, entries[:run]))
		pairs = append(pairs, DirPair{SepName: entries[run-1].Name, ChildKey: leaf[:]})
		entries = entries[run:]
	}
	put(key.DirNode, marshal(t, pairs))
	return keys, store
}

// Whatever a stored body turns into, LookupEntry gives what the decoder
// alone gives: the entry, or the error with its text.
func TestLookupEntryMatchesDecoderOnMutatedTrees(t *testing.T) {
	r := rand.New(rand.NewPCG(0xd1b54a32d192ed03, 0))
	inline := cbor.RawMessage{0xa1, 0x41, 'k', 0x41, 'v'}
	for range 100 {
		names := randNames(r, 1+r.IntN(40))
		entries := make([]Entry, len(names))
		for i, name := range names {
			entries[i] = randEntry(r, name)
			if r.IntN(16) == 0 { // a leaf the scan leaves to the decoder
				entries[i].XattrsKey, entries[i].XattrsIn = nil, inline
			}
		}
		keys, store := testTree(t, r, entries)
		root := keys[len(keys)-1]
		get := func(k key.Key) ([]byte, error) {
			b, ok := store[k]
			if !ok {
				return nil, fmt.Errorf("object %s not in store", k)
			}
			return b, nil
		}
		queries := append(slices.Clone(names), randNames(r, 4)...)
		queries = append(queries, []byte{})
		for _, q := range queries {
			sameLookup(t, root, q, get)
		}
		// The store does not check a body against its key here, so each
		// object in turn is read back damaged.
		for _, k := range keys {
			body := store[k]
			for _, m := range mutations(r, body) {
				store[k] = m
				for range 4 {
					sameLookup(t, root, queries[r.IntN(len(queries))], get)
				}
			}
			store[k] = body
		}
	}
}

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	return b
}

// leafForms are DirLeaf bodies that are not what the encoder writes for
// [{0: h'61', 1: 0o100644, 2: 0, 3: 0, 4: 0}]. decodes tells whether
// DecodeDirLeaf takes the body all the same.
var leafForms = []struct {
	name    string
	body    string
	decodes bool
}{
	{"array head not shortest", "9801 a5 00 4161 01 1981a4 02 00 03 00 04 00", true},
	{"indefinite array", "9f a5 00 4161 01 1981a4 02 00 03 00 04 00 ff", true},
	{"indefinite map", "81 bf 00 4161 01 1981a4 02 00 03 00 04 00 ff", true},
	{"key not shortest", "81 a5 1800 4161 01 1981a4 02 00 03 00 04 00", true},
	{"integer not shortest", "81 a5 00 4161 01 1a000081a4 02 00 03 00 04 00", true},
	{"name length not shortest", "81 a5 00 580161 01 1981a4 02 00 03 00 04 00", true},
	{"indefinite name", "81 a5 00 5f4161ff 01 1981a4 02 00 03 00 04 00", true},
	{"name as an array of bytes", "81 a5 00 811861 01 1981a4 02 00 03 00 04 00", true},
	{"name as text", "81 a5 00 6161 01 1981a4 02 00 03 00 04 00", false},
	{"keys out of order", "81 a5 01 1981a4 00 4161 02 00 03 00 04 00", true},
	{"key repeated", "81 a6 00 4161 00 4162 01 1981a4 02 00 03 00 04 00", true},
	{"required keys out of order", "81 a5 00 4161 02 00 01 1981a4 03 00 04 00", true},
	{"optional keys out of order", "81 a7 00 4161 01 1981a4 02 00 03 00 04 00 06 4162 05 4163", true},
	{"optional key repeated", "81 a7 00 4161 01 1981a4 02 00 03 00 04 00 05 4162 05 4163", true},
	{"required key missing", "81 a4 00 4161 01 1981a4 02 00 03 00", true},
	{"name alone", "81 a1 00 4161", true},
	{"key 10", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 0a 00", true},
	{"negative key", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 20 00", true},
	{"text key", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 6161 00", true},
	{"inline xattrs", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 08 a1 416b 4176", true},
	{"inline xattrs not a map", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 08 00", true},
	{"inline xattrs a byte string", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 08 4100", true},
	{"self-described tag", "d9d9f7 81 a5 00 4161 01 1981a4 02 00 03 00 04 00", true},
	{"tagged entry", "81 d818 a5 00 4161 01 1981a4 02 00 03 00 04 00", true},
	{"null leaf", "f6", true},
	{"undefined leaf", "f7", true},
	{"null entry", "81 f6", true},
	{"entry is an array", "81 80", false},
	{"mode negative", "81 a5 00 4161 01 20 02 00 03 00 04 00", false},
	{"mtime above int64", "81 a5 00 4161 01 1981a4 02 00 03 00 04 1bffffffffffffffff", false},
	{"mtime below int64", "81 a5 00 4161 01 1981a4 02 00 03 00 04 3bffffffffffffffff", false},
	{"mtime a float", "81 a5 00 4161 01 1981a4 02 00 03 00 04 f93c00", false},
	{"rdev of text", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 07 816161", false},
	{"indefinite rdev", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 07 9f0801ff", true},
	{"null content key", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 05 f6", true},
	{"names descend", "82 a5 00 417a 01 00 02 00 03 00 04 00 a5 00 4161 01 00 02 00 03 00 04 00", true},
	{"name repeated", "82 a5 00 4161 01 00 02 00 03 00 04 00 a5 00 4161 01 01 02 00 03 00 04 00", true},
	{"trailing byte", "81 a5 00 4161 01 1981a4 02 00 03 00 04 00 00", false},
	{"truncated", "81 a5 00 4161 01 1981a4 02 00 03 00 04", false},
	{"empty", "", false},
	{"break", "ff", false},
	{"one entry too many", "9a00020001", false},
	{"entries not there", "9a00020000", false},
	{"array of 2^64-1", "9bffffffffffffffff", false},
	{"map of 2^64-1", "81 bbffffffffffffffff", false},
	{"name of 2^64-1 bytes", "81 a5 00 5bffffffffffffffff", false},
	{"name of 2^63-1 bytes", "81 a5 00 5b7fffffffffffffff", false},
	{"name of 2^32-1 bytes", "81 a5 00 5affffffff", false},
	{"rdev of 2^64-1", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 07 9bffffffffffffffff", false},
	{"rdev not there", "81 a6 00 4161 01 1981a4 02 00 03 00 04 00 07 9a00020000", false},
}

// canonLeaf is the DirLeaf leafForms vary, as the encoder writes it.
const canonLeaf = "81 a5 00 4161 01 1981a4 02 00 03 00 04 00"

// The decoder takes many encodings of a leaf besides the canonical one. The
// scan leaves every one of them, and every malformed body, to the decoder,
// without allocating whatever length the body claims.
func TestScanLeafDefersOtherForms(t *testing.T) {
	a := []byte("a")
	canon := unhex(t, canonLeaf)
	if got := marshal(t, []Entry{{Name: a, Mode: 0o100644}}); !bytes.Equal(got, canon) {
		t.Fatalf("the encoder writes %x, the test takes %x for canonical", got, canon)
	}
	if _, res := scanLeaf(canon, a); res != scanFound {
		t.Fatalf("scanLeaf of the canonical leaf = %d, want scanFound", res)
	}
	leaf, err := key.New(key.DirLeaf, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range leafForms {
		b := unhex(t, tc.body)
		if _, err := DecodeDirLeaf(b); (err == nil) != tc.decodes {
			t.Errorf("%s: DecodeDirLeaf error = %v, want decodes = %v", tc.name, err, tc.decodes)
		}
		for _, name := range [][]byte{a, []byte("z"), {}} {
			if _, res := scanLeaf(b, name); res != scanUnsure {
				t.Errorf("%s: scanLeaf(%q) = %d, want scanUnsure", tc.name, name, res)
			}
			sameLookup(t, leaf, name, func(key.Key) ([]byte, error) { return b, nil })
		}
		if n := testing.AllocsPerRun(10, func() { scanLeaf(b, a) }); n != 0 {
			t.Errorf("%s: scanLeaf allocates %v times", tc.name, n)
		}
	}
}

// The scan reads a grammar, not the encoder's exact output. It answers at the
// edges of the canonical form, and for bodies the encoder never writes (a
// required key left out, an empty value under an optional key) that the
// decoder reads in one way only.
func TestScanLeafAnswersWhereTheDecoderHasOneReading(t *testing.T) {
	leaf, err := key.New(key.DirLeaf, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, body string }{
		{"empty leaf", "80"},
		{"empty name", "81 a5 00 40 01 00 02 00 03 00 04 00"},
		{"mtime missing, content key empty", "81 a5 00 4161 01 00 02 00 03 00 05 40"},
		{"largest mtime", "81 a5 00 4161 01 00 02 00 03 00 04 1b7fffffffffffffff"},
		{"smallest mtime", "81 a5 00 4161 01 00 02 00 03 00 04 3b7fffffffffffffff"},
		{"empty rdev", "81 a6 00 4161 01 00 02 00 03 00 04 00 07 80"},
		{"rdev of three", "81 a6 00 4161 01 00 02 00 03 00 04 00 07 83 00 01 1818"},
		{"every key but 8", "81 a9 00 4161 01 00 02 00 03 00 04 00 05 4101 06 4102 07 82 00 01 09 4103"},
	} {
		b := unhex(t, tc.body)
		dec, decErr := DecodeDirLeaf(b)
		for _, name := range [][]byte{[]byte("a"), []byte("z"), {}} {
			if !agreesLeaf(t, b, name, dec, decErr) {
				t.Errorf("%s: left to the decoder for %q", tc.name, name)
			}
			sameLookup(t, leaf, name, func(key.Key) ([]byte, error) { return b, nil })
		}
	}
}

// An integer is canonical in the narrowest head that holds it. The decoder
// reads every width alike; the scan takes the narrowest only.
func TestScanTakesOnlyTheShortestHead(t *testing.T) {
	a := []byte("a")
	for _, v := range []uint64{0, 23, 24, 255, 256, 65535, 65536, 1<<32 - 1, 1 << 32, 1<<64 - 1} {
		shortest := true // widths ascend, so the first that holds v
		for _, w := range []struct {
			ai    byte
			width int
		}{{0, 0}, {24, 1}, {25, 2}, {26, 4}, {27, 8}} {
			if w.width == 0 && v > 23 || w.width > 0 && w.width < 8 && v>>(8*w.width) != 0 {
				continue
			}
			uid := []byte{w.ai} // the head of an unsigned integer
			if w.width == 0 {
				uid[0] = byte(v) // below 24 the head holds the value
			}
			for i := w.width - 1; i >= 0; i-- {
				uid = append(uid, byte(v>>(8*i)))
			}
			body := slices.Concat(unhex(t, "81 a5 00 4161 01 00 02"), uid, unhex(t, "03 00 04 00"))
			dec, err := DecodeDirLeaf(body)
			if err != nil || len(dec) != 1 || dec[0].UID != v {
				t.Fatalf("DecodeDirLeaf(%x) = %+v, %v, want uid %d", body, dec, err, v)
			}
			want := scanUnsure
			if shortest {
				want = scanFound
			}
			if _, res := scanLeaf(body, a); res != want {
				t.Errorf("scanLeaf(%x) = %d, want %d", body, res, want)
			}
			agreesLeaf(t, body, a, dec, err)
			shortest = false
		}
	}
}

// nodeForms are DirNode bodies that are not what the encoder writes for
// [[h'61', K]], K standing for a child key. decodes tells whether
// DecodeDirNode takes the body all the same.
var nodeForms = []struct {
	name    string
	body    string
	decodes bool
}{
	{"array head not shortest", "9801 82 4161 K", true},
	{"indefinite array", "9f 82 4161 K ff", true},
	{"indefinite pair", "81 9f 4161 K ff", true},
	{"separator length not shortest", "81 82 580161 K", true},
	{"indefinite separator", "81 82 5f4161ff K", true},
	{"separator as text", "81 82 6161 K", false},
	{"child key as text", "81 82 4161 6161", false},
	{"pair of three", "81 83 4161 K 00", false},
	{"pair of one", "81 81 4161", false},
	{"pair is a map", "81 a1 4161 K", false},
	{"self-described tag", "d9d9f7 81 82 4161 K", true},
	{"tagged pair", "81 d818 82 4161 K", true},
	{"null node", "f6", true},
	{"null pair", "81 f6", true},
	{"separators descend", "82 82 417a K 82 4161 K", true},
	{"separator repeated", "82 82 4161 K 82 4161 K", true},
	{"trailing byte", "81 82 4161 K 00", false},
	{"truncated", "81 82 4161 5820 00", false},
	{"empty", "", false},
	{"break", "ff", false},
	{"one pair too many", "9a00020001", false},
	{"pairs not there", "9a00020000", false},
	{"array of 2^64-1", "9bffffffffffffffff", false},
	{"separator of 2^64-1 bytes", "81 82 5bffffffffffffffff", false},
	{"child key of 2^63-1 bytes", "81 82 4161 5b7fffffffffffffff", false},
}

// canonNode is the DirNode nodeForms vary, as the encoder writes it.
const canonNode = "81 82 4161 K"

// nodeFormStore returns a store holding the canonical leaf, and the node
// bodies' K: that leaf's key as a CBOR byte string, in hex.
func nodeFormStore(t testing.TB) (get func(key.Key) ([]byte, error), k string) {
	t.Helper()
	leaf := unhex(t, canonLeaf)
	lk, err := key.New(key.DirLeaf, uint64(len(leaf)), leaf)
	if err != nil {
		t.Fatal(err)
	}
	get = func(k key.Key) ([]byte, error) {
		if k != lk {
			return nil, fmt.Errorf("object %s not in store", k)
		}
		return leaf, nil
	}
	return get, "5820" + hex.EncodeToString(lk[:])
}

func TestScanNodeDefersOtherForms(t *testing.T) {
	a := []byte("a")
	leafGet, k := nodeFormStore(t)
	canon := unhex(t, strings.ReplaceAll(canonNode, "K", k))
	if got := marshal(t, []DirPair{{SepName: a, ChildKey: unhex(t, k)[2:]}}); !bytes.Equal(got, canon) {
		t.Fatalf("the encoder writes %x, the test takes %x for canonical", got, canon)
	}
	if _, res := scanNode(canon, a); res != scanFound {
		t.Fatalf("scanNode of the canonical node = %d, want scanFound", res)
	}
	node, err := key.New(key.DirNode, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range nodeForms {
		b := unhex(t, strings.ReplaceAll(tc.body, "K", k))
		if _, err := DecodeDirNode(b); (err == nil) != tc.decodes {
			t.Errorf("%s: DecodeDirNode error = %v, want decodes = %v", tc.name, err, tc.decodes)
		}
		get := func(k key.Key) ([]byte, error) {
			if k == node {
				return b, nil
			}
			return leafGet(k)
		}
		for _, name := range [][]byte{a, []byte("z"), {}} {
			if _, res := scanNode(b, name); res != scanUnsure {
				t.Errorf("%s: scanNode(%q) = %d, want scanUnsure", tc.name, name, res)
			}
			sameLookup(t, node, name, get)
		}
		if n := testing.AllocsPerRun(10, func() { scanNode(b, a) }); n != 0 {
			t.Errorf("%s: scanNode allocates %v times", tc.name, n)
		}
	}
}

// The decoder takes an array of up to scanMaxItems elements, and so does the
// scan; one more and both refuse.
func TestScanTakesAsManyItemsAsTheDecoder(t *testing.T) {
	head := func(n int) []byte { return []byte{0x9a, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)} }
	name := func(i int) []byte { return []byte{byte(i >> 16), byte(i >> 8), byte(i)} }
	leaf := func(n int) []byte {
		b := head(n)
		for i := range n {
			b = append(b, 0xa5, 0x00, 0x43)
			b = append(b, name(i)...)
			b = append(b, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00)
		}
		return b
	}
	node := func(n int) []byte {
		b := head(n)
		for i := range n {
			b = append(b, 0x82, 0x43)
			b = append(b, name(i)...)
			b = append(b, 0x41, byte(i))
		}
		return b
	}
	last := name(scanMaxItems - 1)

	b := leaf(scanMaxItems)
	dec, err := DecodeDirLeaf(b)
	if err != nil || len(dec) != scanMaxItems {
		t.Fatalf("DecodeDirLeaf of %d entries: %d entries, %v", scanMaxItems, len(dec), err)
	}
	if e, res := scanLeafEntry(b, last); res != scanFound || !reflect.DeepEqual(e, dec[len(dec)-1]) {
		t.Errorf("scanLeafEntry of %d entries = %+v, %d", scanMaxItems, e, res)
	}
	if n := testing.AllocsPerRun(1, func() { scanLeaf(b, last) }); n != 0 {
		t.Errorf("scanLeaf allocates %v times", n)
	}
	b = leaf(scanMaxItems + 1)
	if _, err := DecodeDirLeaf(b); err == nil {
		t.Errorf("DecodeDirLeaf took %d entries", scanMaxItems+1)
	}
	if _, res := scanLeaf(b, last); res != scanUnsure {
		t.Errorf("scanLeaf of %d entries = %d, want scanUnsure", scanMaxItems+1, res)
	}

	b = node(scanMaxItems)
	pairs, err := DecodeDirNode(b)
	if err != nil || len(pairs) != scanMaxItems {
		t.Fatalf("DecodeDirNode of %d pairs: %d pairs, %v", scanMaxItems, len(pairs), err)
	}
	if child, res := scanNode(b, last); res != scanFound || !bytes.Equal(child, pairs[len(pairs)-1].ChildKey) {
		t.Errorf("scanNode of %d pairs = %x, %d", scanMaxItems, child, res)
	}
	if n := testing.AllocsPerRun(1, func() { scanNode(b, last) }); n != 0 {
		t.Errorf("scanNode allocates %v times", n)
	}
	b = node(scanMaxItems + 1)
	if _, err := DecodeDirNode(b); err == nil {
		t.Errorf("DecodeDirNode took %d pairs", scanMaxItems+1)
	}
	if _, res := scanNode(b, last); res != scanUnsure {
		t.Errorf("scanNode of %d pairs = %d, want scanUnsure", scanMaxItems+1, res)
	}
}

// kindsOfEntries returns one entry of every kind ingest writes, in name
// order, none with inline xattrs.
func kindsOfEntries(t testing.TB) []Entry {
	t.Helper()
	blob, err := EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	xattrs, err := EncodeXattrSet(map[string][]byte{"user.k": []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	return []Entry{
		{Name: []byte("blk"), Mode: 0o060660, GID: 6, Rdev: []uint64{8, 1}},
		{Name: []byte("dir"), Mode: 0o040755, UID: 1000, GID: 100, Mtime: -1, ContentKey: blob.Key[:]},
		{Name: []byte("fifo"), Mode: 0o010644, Mtime: 1 << 62},
		{Name: []byte("file"), Mode: 0o100644, UID: 1 << 40, Mtime: 1_700_000_000_000_000_000, ContentKey: blob.Key[:], XattrsKey: xattrs.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, Mtime: -1 << 63, LinkTarget: []byte("dir/file")},
	}
}

// What the builders write is the canonical form: no object of a tree they
// build is left to the decoder.
func TestScanTakesWhatTheBuildersWrite(t *testing.T) {
	kinds := kindsOfEntries(t)
	var objs []Object
	emit := func(o Object) error {
		objs = append(objs, o)
		return nil
	}
	db := NewDirBuilder(chunkers.NewItemChunker(2))
	var names [][]byte
	for i := range 2000 {
		e := kinds[i%len(kinds)]
		e.Name = fmt.Appendf(nil, "%05d-%s", i, e.Name)
		names = append(names, e.Name)
		if err := db.AddEntry(emit, e); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(emit)
	if err != nil {
		t.Fatal(err)
	}
	store := map[key.Key][]byte{}
	var leaves, nodes int
	for _, o := range objs {
		store[o.Key] = o.Bytes
		for _, name := range [][]byte{names[0], names[len(names)/2], []byte("zzz"), {}} {
			var res scanResult
			switch o.Key.Type() {
			case key.DirLeaf:
				_, res = scanLeaf(o.Bytes, name)
			case key.DirNode:
				_, res = scanNode(o.Bytes, name)
			}
			if res == scanUnsure {
				t.Fatalf("%v %s left to the decoder for %q", o.Key.Type(), o.Key, name)
			}
		}
		switch o.Key.Type() {
		case key.DirLeaf:
			leaves++
		case key.DirNode:
			nodes++
		}
	}
	if leaves < 2 || nodes < 2 {
		t.Fatalf("fixture has %d leaves and %d nodes, want several of each", leaves, nodes)
	}
	get := func(k key.Key) ([]byte, error) { return store[k], nil }
	for _, name := range append(names, []byte("zzz"), []byte{}, []byte("00000"), []byte("00007-dir~")) {
		sameLookup(t, root, name, get)
	}

	// And LookupEntry stops at the scan's answer: it allocates the entry it
	// returns, or its error, where the decoder allocates every entry of
	// every object on the way down. The last name misses in the root.
	for _, name := range [][]byte{names[len(names)/2], []byte("00007-dir~"), []byte("zzz")} {
		scanned := testing.AllocsPerRun(20, func() { LookupEntry(root, name, get) })
		decoded := testing.AllocsPerRun(20, func() { lookupEntryDecoded(root, name, get) })
		if scanned > 8 || decoded <= scanned {
			t.Errorf("LookupEntry(%q) allocates %v times, the decoder alone %v times", name, scanned, decoded)
		}
	}
}

// The entry LookupEntry returns owns its bytes, as the decoder's does: a
// caller may keep it after the buffer the object was read into is reused.
func TestLookupEntryDoesNotAliasTheObject(t *testing.T) {
	for _, want := range kindsOfEntries(t) {
		body := marshal(t, []Entry{want})
		leaf, err := key.New(key.DirLeaf, uint64(len(body)), body)
		if err != nil {
			t.Fatal(err)
		}
		if _, res := scanLeaf(body, want.Name); res != scanFound {
			t.Fatalf("%s: scanLeaf = %d, want scanFound", want.Name, res)
		}
		for _, lookup := range []func(key.Key, []byte, func(key.Key) ([]byte, error)) (Entry, error){LookupEntry, lookupEntryDecoded} {
			buf := bytes.Clone(body)
			got, err := lookup(leaf, want.Name, func(key.Key) ([]byte, error) { return buf, nil })
			if err != nil {
				t.Fatal(err)
			}
			for i := range buf {
				buf[i] = 0xff
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: entry after its object's buffer was overwritten = %+v, want %+v", want.Name, got, want)
			}
		}
	}
}

// fuzzSeeds are DirLeaf and DirNode bodies, canonical and not, to start both
// fuzz targets from.
func fuzzSeeds(t testing.TB) (bodies, names [][]byte) {
	t.Helper()
	_, k := nodeFormStore(t)
	kinds := kindsOfEntries(t)
	bodies = [][]byte{
		unhex(t, canonLeaf),
		unhex(t, strings.ReplaceAll(canonNode, "K", k)),
		marshal(t, kinds),
		marshal(t, []Entry{}),
		marshal(t, []DirPair{
			{SepName: []byte("a"), ChildKey: unhex(t, k)[2:]},
			{SepName: []byte("file"), ChildKey: unhex(t, k)[2:]},
			{SepName: []byte("zz"), ChildKey: unhex(t, k)[2:]},
		}),
	}
	for _, tc := range leafForms {
		bodies = append(bodies, unhex(t, tc.body))
	}
	for _, tc := range nodeForms {
		bodies = append(bodies, unhex(t, strings.ReplaceAll(tc.body, "K", k)))
	}
	names = [][]byte{{}, []byte("a"), []byte("b"), []byte("zzz")}
	for _, e := range kinds {
		names = append(names, e.Name)
	}
	return bodies, names
}

// FuzzScanLeaf reads any bytes as a DirLeaf. The scan must not panic; when it
// answers the decoder must agree, and LookupEntry must give what the decoder
// alone gives either way.
func FuzzScanLeaf(f *testing.F) {
	bodies, names := fuzzSeeds(f)
	for _, b := range bodies {
		for _, n := range names {
			f.Add(b, n)
		}
	}
	leaf, err := key.New(key.DirLeaf, 0, nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, body, name []byte) {
		dec, decErr := DecodeDirLeaf(body)
		agreesLeaf(t, body, name, dec, decErr)
		sameLookup(t, leaf, name, func(key.Key) ([]byte, error) { return body, nil })
	})
}

// FuzzScanNode is FuzzScanLeaf for a DirNode, over a store that holds the
// canonical leaf for its child keys to point at.
func FuzzScanNode(f *testing.F) {
	bodies, names := fuzzSeeds(f)
	for _, b := range bodies {
		for _, n := range names {
			f.Add(b, n)
		}
	}
	leafGet, _ := nodeFormStore(f)
	node, err := key.New(key.DirNode, 0, nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, body, name []byte) {
		dec, decErr := DecodeDirNode(body)
		agreesNode(t, body, name, dec, decErr)
		sameLookup(t, node, name, func(k key.Key) ([]byte, error) {
			if k == node {
				return body, nil
			}
			return leafGet(k)
		})
	})
}

// benchEntries returns n regular-file entries in name order, shaped like the
// ones ingest writes: a mode, an owner, an mtime and a content key.
func benchEntries(tb testing.TB, n int) []Entry {
	tb.Helper()
	entries := make([]Entry, n)
	for i := range entries {
		name := fmt.Appendf(nil, "pkg-%06d.nix", i)
		ck, err := key.New(key.Blob, uint64(1000+i), name)
		if err != nil {
			tb.Fatal(err)
		}
		entries[i] = Entry{
			Name:       name,
			Mode:       0o100644,
			UID:        1000,
			GID:        100,
			Mtime:      1_700_000_000_000_000_000 + int64(i),
			ContentKey: ck[:],
		}
	}
	return entries
}

// BenchmarkLookupEntry looks names up in three directories: a small one and
// one as wide as a single DirLeaf gets under ingest's default item chunker
// (average run 2^7, at most 512 entries), and one of 100,000 entries, a
// DirNode tree three objects deep. Absent names sort right after a present
// one, so the lookup walks down to a leaf before it misses. The fourth is the
// wide leaf with inline xattrs on its last entry: the scan reads all of it
// and then leaves it to the decoder, the dearest a lookup gets.
func BenchmarkLookupEntry(b *testing.B) {
	const itemBits = 7 // ingest.DefaultItemBits
	store := map[key.Key][]byte{}
	emit := func(o Object) error {
		store[o.Key] = o.Bytes
		return nil
	}
	get := func(k key.Key) ([]byte, error) {
		data, ok := store[k]
		if !ok {
			return nil, fmt.Errorf("object %s not in store", k)
		}
		return data, nil
	}

	leaf := func(n int) (key.Key, []Entry) {
		entries := benchEntries(b, n)
		o, err := EncodeDirLeaf(entries)
		if err != nil {
			b.Fatal(err)
		}
		if err := emit(o); err != nil {
			b.Fatal(err)
		}
		return o.Key, entries
	}
	xattrLeaf := func(n int) (key.Key, []Entry) {
		entries := benchEntries(b, n)
		entries[n-1].XattrsIn = cbor.RawMessage{0xa1, 0x41, 'k', 0x41, 'v'}
		o, err := EncodeDirLeaf(entries)
		if err != nil {
			b.Fatal(err)
		}
		if err := emit(o); err != nil {
			b.Fatal(err)
		}
		return o.Key, entries
	}
	tree := func(n int) (key.Key, []Entry) {
		entries := benchEntries(b, n)
		db := NewDirBuilder(chunkers.NewItemChunker(itemBits))
		for _, e := range entries {
			if err := db.AddEntry(emit, e); err != nil {
				b.Fatal(err)
			}
		}
		root, err := db.Finish(emit)
		if err != nil {
			b.Fatal(err)
		}
		if root.Type() != key.DirNode {
			b.Fatalf("fixture root = %v, want a DirNode", root.Type())
		}
		return root, entries
	}

	for _, tc := range []struct {
		name  string
		build func(int) (key.Key, []Entry)
		n     int
	}{
		{"leaf8", leaf, 8},
		{"leaf512", leaf, 512},
		{"tree100k", tree, 100_000},
		{"leaf512xattrs", xattrLeaf, 512},
	} {
		root, entries := tc.build(tc.n)
		present := make([][]byte, len(entries))
		absent := make([][]byte, len(entries))
		for i, e := range entries {
			present[i] = e.Name
			absent[i] = append(append([]byte(nil), e.Name...), '~')
		}
		// A stride coprime to every fixture size spreads the lookups over
		// the whole directory.
		const stride = 7919
		b.Run(tc.name+"/present", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, err := LookupEntry(root, present[i], get); err != nil {
					b.Fatal(err)
				}
				i = (i + stride) % len(present)
			}
		})
		b.Run(tc.name+"/absent", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, err := LookupEntry(root, absent[i], get); err == nil {
					b.Fatalf("found %q", absent[i])
				}
				i = (i + stride) % len(absent)
			}
		})
	}
}
