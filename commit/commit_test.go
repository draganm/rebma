package commit_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/key"
)

const (
	annWhen = int64(1_700_000_000_000_000_000)
	bobWhen = annWhen + 1
)

var (
	ann = commit.Identity{Name: "Ann", Email: "ann@example.com", When: annWhen, TZOffset: 120}
	bob = commit.Identity{Name: "Bob", Email: "", When: bobWhen, TZOffset: -300}
)

// emptyDir is the key of the empty directory: a DirLeaf whose body is the
// empty CBOR array 0x80, length field 1.
func emptyDir(t *testing.T) key.Key {
	t.Helper()
	k, err := key.New(key.DirLeaf, 1, []byte{0x80})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// root returns a parentless commit of the empty directory and its key.
func root(t *testing.T, msg string) (key.Key, commit.Commit) {
	t.Helper()
	c := commit.Commit{Tree: emptyDir(t), Author: ann, Committer: ann, Message: msg}
	k, _, err := c.Object()
	if err != nil {
		t.Fatal(err)
	}
	return k, c
}

// merge is the fixed vector commit: parents root("a") and root("b").
func merge(t *testing.T) commit.Commit {
	t.Helper()
	ka, _ := root(t, "a")
	kb, _ := root(t, "b")
	return commit.Commit{
		Tree:      emptyDir(t),
		Parents:   []key.Key{ka, kb},
		Author:    ann,
		Committer: bob,
		Message:   "merge\n",
	}
}

// Hand-rolled CBOR, independent of the library under test.
func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
func bstr32(k key.Key) []byte    { return append([]byte{0x58, 0x20}, k[:]...) }
func u64(n int64) []byte         { return binary.BigEndian.AppendUint64([]byte{0x1b}, uint64(n)) }
func tstr(s string) []byte {
	if len(s) >= 24 {
		panic("tstr: short strings only")
	}
	return append([]byte{0x60 | byte(len(s))}, s...)
}

// handMerge assembles merge(t)'s canonical bytes from the spec's tables.
func handMerge(t *testing.T) []byte {
	t.Helper()
	ka, _ := root(t, "a")
	kb, _ := root(t, "b")
	return cat(
		[]byte{0xa5}, // map(5)
		[]byte{0x00}, bstr32(emptyDir(t)),
		[]byte{0x01, 0x82}, bstr32(ka), bstr32(kb), // array(2)
		[]byte{0x02, 0xa4, 0x00}, tstr("Ann"), []byte{0x01}, tstr("ann@example.com"),
		[]byte{0x02}, u64(annWhen), []byte{0x03, 0x18, 0x78}, // +120
		[]byte{0x03, 0xa4, 0x00}, tstr("Bob"), []byte{0x01}, tstr(""),
		[]byte{0x02}, u64(bobWhen), []byte{0x03, 0x39, 0x01, 0x2b}, // -300
		[]byte{0x04}, tstr("merge\n"),
	)
}

// replaceOnce swaps the single occurrence of old in b.
func replaceOnce(t *testing.T, b, old, new []byte) []byte {
	t.Helper()
	if n := bytes.Count(b, old); n != 1 {
		t.Fatalf("pattern % x occurs %d times, want 1", old, n)
	}
	return bytes.Replace(b, old, new, 1)
}

func TestEncodeMatchesHandAssembledBytes(t *testing.T) {
	got, err := merge(t).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if want := handMerge(t); !bytes.Equal(got, want) {
		t.Fatalf("encoding differs from the spec\n got: %x\nwant: %x", got, want)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	signed := merge(t)
	signed.Signature = []byte("sig")
	signed.PublicKey = []byte("pub")
	_, rootCommit := root(t, "")
	for name, c := range map[string]commit.Commit{"merge": merge(t), "signed": signed, "root, empty message": rootCommit} {
		b, err := c.Encode()
		if err != nil {
			t.Fatalf("%s: Encode: %v", name, err)
		}
		got, err := commit.Decode(b)
		if err != nil {
			t.Fatalf("%s: Decode: %v", name, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Errorf("%s: round trip\n got: %+v\nwant: %+v", name, got, c)
		}
	}
}

func TestObjectKey(t *testing.T) {
	k, b, err := merge(t).Object()
	if err != nil {
		t.Fatal(err)
	}
	if k.Type() != key.Commit {
		t.Errorf("key type = %v, want Commit", k.Type())
	}
	footprint := uint64(len(b)) + emptyDir(t).Length()
	if k.Length() != footprint {
		t.Errorf("key length = %d, want own bytes plus the tree's length, %d", k.Length(), footprint)
	}
	want, err := key.New(key.Commit, footprint, b)
	if err != nil {
		t.Fatal(err)
	}
	if k != want {
		t.Errorf("key = %s, want %s", k, want)
	}
}

func TestSignaturePayload(t *testing.T) {
	c := merge(t)
	c.PublicKey = []byte("pub")
	unsigned, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	c.Signature = []byte("sig")
	payload, err := c.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, unsigned) {
		t.Error("payload is not the encoding without the signature")
	}
	if full, _ := c.Encode(); bytes.Equal(full, payload) {
		t.Error("payload still carries the signature")
	}
	c.PublicKey = []byte("other")
	if other, _ := c.SignaturePayload(); bytes.Equal(other, payload) {
		t.Error("payload does not cover the public key")
	}
}

// commitKeys returns n distinct canonical Commit-type keys.
func commitKeys(t *testing.T, n int) []key.Key {
	t.Helper()
	out := make([]key.Key, n)
	for i := range out {
		var h [32]byte
		binary.BigEndian.PutUint32(h[:], uint32(i+1))
		k, err := key.NewFromHash(key.Commit, 1, h)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = k
	}
	return out
}

func TestEncodeAcceptsBounds(t *testing.T) {
	cases := map[string]func(*commit.Commit){
		"max parents":      func(c *commit.Commit) { c.Parents = commitKeys(t, commit.MaxParents) },
		"tz upper":         func(c *commit.Commit) { c.Author.TZOffset = commit.MaxTZOffset },
		"tz lower":         func(c *commit.Commit) { c.Author.TZOffset = -commit.MaxTZOffset },
		"max message":      func(c *commit.Commit) { c.Message = strings.Repeat("m", commit.MaxMessageLen) },
		"multiline":        func(c *commit.Commit) { c.Message = "subject\n\n\tbody\n" },
		"max name":         func(c *commit.Commit) { c.Author.Name = strings.Repeat("n", commit.MaxIdentityLen) },
		"negative time":    func(c *commit.Commit) { c.Author.When = -1 },
		"max signature":    func(c *commit.Commit) { c.Signature = make([]byte, commit.MaxSignatureLen) },
		"max public key":   func(c *commit.Commit) { c.PublicKey = make([]byte, commit.MaxPublicKeyLen) },
		"dir node as tree": func(c *commit.Commit) { c.Tree, _ = key.NewFromHash(key.DirNode, 9, [32]byte{1}) },
		"empty names":      func(c *commit.Commit) { c.Author.Name, c.Committer.Name = "", "" },
	}
	for name, mutate := range cases {
		c := merge(t)
		mutate(&c)
		b, err := c.Encode()
		if err != nil {
			t.Errorf("%s: Encode: %v", name, err)
			continue
		}
		if _, err := commit.Decode(b); err != nil {
			t.Errorf("%s: Decode: %v", name, err)
		}
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	fileNode, _ := key.NewFromHash(key.FileNode, 9, [32]byte{1})
	cases := map[string]func(*commit.Commit){
		"zero tree":             func(c *commit.Commit) { c.Tree = key.Key{} },
		"file tree":             func(c *commit.Commit) { c.Tree = fileNode },
		"non-canonical tree":    func(c *commit.Commit) { c.Tree[31] |= 0x08 },
		"parent not a commit":   func(c *commit.Commit) { c.Parents[0] = c.Tree },
		"duplicate parents":     func(c *commit.Commit) { c.Parents[1] = c.Parents[0] },
		"too many parents":      func(c *commit.Commit) { c.Parents = commitKeys(t, commit.MaxParents+1) },
		"control char in name":  func(c *commit.Commit) { c.Author.Name = "An\nn" },
		"DEL in email":          func(c *commit.Commit) { c.Author.Email = "a\x7fb" },
		"invalid UTF-8 name":    func(c *commit.Commit) { c.Author.Name = "A\xffnn" },
		"name too long":         func(c *commit.Commit) { c.Author.Name = strings.Repeat("n", commit.MaxIdentityLen+1) },
		"email too long":        func(c *commit.Commit) { c.Committer.Email = strings.Repeat("e", commit.MaxIdentityLen+1) },
		"tz too far east":       func(c *commit.Commit) { c.Author.TZOffset = commit.MaxTZOffset + 1 },
		"tz too far west":       func(c *commit.Commit) { c.Committer.TZOffset = -commit.MaxTZOffset - 1 },
		"message too long":      func(c *commit.Commit) { c.Message = strings.Repeat("m", commit.MaxMessageLen+1) },
		"invalid UTF-8 message": func(c *commit.Commit) { c.Message = "bad \xff" },
		"signature too long":    func(c *commit.Commit) { c.Signature = make([]byte, commit.MaxSignatureLen+1) },
		"public key too long":   func(c *commit.Commit) { c.PublicKey = make([]byte, commit.MaxPublicKeyLen+1) },
	}
	for name, mutate := range cases {
		c := merge(t)
		mutate(&c)
		if _, err := c.Encode(); err == nil {
			t.Errorf("%s: Encode accepted an invalid commit", name)
		}
		if _, _, err := c.Object(); err == nil {
			t.Errorf("%s: Object accepted an invalid commit", name)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	good := handMerge(t)
	if _, err := commit.Decode(good); err != nil {
		t.Fatalf("baseline must decode: %v", err)
	}
	ka, _ := root(t, "a")
	kb, _ := root(t, "b")
	tree := emptyDir(t)
	message := cat([]byte{0x04}, tstr("merge\n"))

	withHeader := func(b []byte, h byte) []byte {
		out := bytes.Clone(b)
		out[0] = h
		return out
	}
	cases := map[string][]byte{
		"garbage":       []byte("not cbor at all"),
		"empty":         {},
		"trailing byte": append(bytes.Clone(good), 0x00),
		// Accepted by the CBOR library's defaults; only the canonical
		// re-encoding check stands between these and a second encoding of
		// the same commit.
		"self-described tag": append([]byte{0xd9, 0xd9, 0xf7}, good...),
		"duplicate key 4":    append(withHeader(good, 0xa6), message...),
		"bignum-tagged tz":   replaceOnce(t, good, []byte{0x03, 0x18, 0x78}, []byte{0x03, 0xc2, 0x41, 0x78}),
		"key 7 not bytes":    append(withHeader(good, 0xa6), 0x07, 0x00),
		"unknown key 10":     append(withHeader(good, 0xa6), 0x0a, 0x00),
		"missing message":    withHeader(bytes.TrimSuffix(good, message), 0xa4),
		"non-minimal tz":     replaceOnce(t, good, []byte{0x03, 0x18, 0x78}, []byte{0x03, 0x19, 0x00, 0x78}),
		"tz out of range":    replaceOnce(t, good, []byte{0x03, 0x18, 0x78}, []byte{0x03, 0x19, 0x05, 0xa0}), // 1440
		"identity missing email": replaceOnce(t, good,
			cat([]byte{0xa4, 0x00}, tstr("Bob"), []byte{0x01}, tstr("")),
			cat([]byte{0xa3, 0x00}, tstr("Bob"))),
		"keys out of order": replaceOnce(t, good,
			cat([]byte{0x00}, bstr32(tree), []byte{0x01, 0x82}, bstr32(ka), bstr32(kb)),
			cat([]byte{0x01, 0x82}, bstr32(ka), bstr32(kb), []byte{0x00}, bstr32(tree))),
		"parents null":       replaceOnce(t, good, cat([]byte{0x01, 0x82}, bstr32(ka), bstr32(kb)), []byte{0x01, 0xf6}),
		"indefinite parents": replaceOnce(t, good, cat([]byte{0x01, 0x82}, bstr32(ka), bstr32(kb)), cat([]byte{0x01, 0x9f}, bstr32(ka), bstr32(kb), []byte{0xff})),
		"parent is a tree":   replaceOnce(t, good, cat(bstr32(ka), bstr32(kb)), cat(bstr32(tree), bstr32(kb))),
		"duplicate parent":   replaceOnce(t, good, cat(bstr32(ka), bstr32(kb)), cat(bstr32(ka), bstr32(ka))),
		"short tree key":     replaceOnce(t, good, cat([]byte{0x00}, bstr32(tree)), cat([]byte{0x00, 0x58, 0x1f}, tree[:31])),
		"message as bytes":   replaceOnce(t, good, message, cat([]byte{0x04, 0x46}, []byte("merge\n"))),
	}
	for name, b := range cases {
		if _, err := commit.Decode(b); err == nil {
			t.Errorf("%s: Decode accepted % x", name, b)
		}
	}
}

// Golden vector: the fixed merge commit, pinned as literal bytes so a CBOR
// library upgrade cannot silently change the wire format, and so other
// implementations (core-rs) can adopt it. Derivation, all from this file:
// tree = key.New(DirLeaf, 1, {0x80}); parents = the keys of two root commits of
// that tree by Ann (committer Ann) with messages "a" and "b"; author Ann,
// committer Bob, message "merge\n". A key's length field is the commit's own
// bytes plus its tree's length: 115+1 for the parents, 174+1 for the merge.
const (
	goldenParentA = "8f2a0d816398cc068c636a2859d576a0706ee8e33cd4673e14e81ac87de97450"
	goldenParentB = "68e165549a26b82280724ed57cdf1f8e7e2390a3b37a8ba9a17cd44becbc7450"
	goldenBytes   = "a50058206bf3580bff464ed85a9dce79a9f1c21aedb0e981034d1f6a14a0f5a9e6bb0120018258208f2a0d816398cc068c636a2859d576a0706ee8e33cd4673e14e81ac87de97450582068e165549a26b82280724ed57cdf1f8e7e2390a3b37a8ba9a17cd44becbc745002a40063416e6e016f616e6e406578616d706c652e636f6d021b17979cfe362a000003187803a40063426f620160021b17979cfe362a00010339012b04666d657267650a"
	goldenKey     = "0bfeb34dc3a09fb16c88cd479a7f931a223b73f3836e81c0f9f601c720acaf50"
)

func TestGoldenVector(t *testing.T) {
	ka, _ := root(t, "a")
	kb, _ := root(t, "b")
	k, b, err := merge(t).Object()
	if err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]string{
		"parent a": {ka.String(), goldenParentA},
		"parent b": {kb.String(), goldenParentB},
		"bytes":    {hex.EncodeToString(b), goldenBytes},
		"key":      {k.String(), goldenKey},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s changed!\n got: %s\nwant: %s", name, pair[0], pair[1])
		}
	}
}

// dirKey fabricates a directory key: the codec never fetches a tree, so any
// canonical DirLeaf or DirNode key will do.
func dirKey(t *testing.T, typ key.Type, length uint64, fill byte) key.Key {
	t.Helper()
	var h [32]byte
	for i := range h {
		h[i] = fill
	}
	k, err := key.NewFromHash(typ, length, h)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

var changeID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

// conflicted is the second fixed vector: a three-term conflict with labels, a
// change id, and a committer without a name.
func conflicted(t *testing.T) commit.Commit {
	t.Helper()
	ka, _ := root(t, "a")
	return commit.Commit{
		Tree:           emptyDir(t),
		Parents:        []key.Key{ka},
		Author:         ann,
		Committer:      commit.Identity{Name: "", Email: "bot@example.com", When: bobWhen, TZOffset: -300},
		Message:        "conflict\n",
		ChangeID:       changeID,
		ConflictTerms:  []key.Key{dirKey(t, key.DirLeaf, 300, 0x11), dirKey(t, key.DirNode, 70000, 0x22)},
		ConflictLabels: []string{"ours", "", "theirs"},
	}
}

// handConflicted assembles conflicted(t)'s canonical bytes from the spec's tables.
func handConflicted(t *testing.T) []byte {
	t.Helper()
	ka, _ := root(t, "a")
	c := conflicted(t)
	return cat(
		[]byte{0xa8}, // map(8): keys 0-4, 7, 8, 9
		[]byte{0x00}, bstr32(emptyDir(t)),
		[]byte{0x01, 0x81}, bstr32(ka),
		[]byte{0x02, 0xa4, 0x00}, tstr("Ann"), []byte{0x01}, tstr("ann@example.com"),
		[]byte{0x02}, u64(annWhen), []byte{0x03, 0x18, 0x78},
		[]byte{0x03, 0xa4, 0x00}, tstr(""), []byte{0x01}, tstr("bot@example.com"),
		[]byte{0x02}, u64(bobWhen), []byte{0x03, 0x39, 0x01, 0x2b},
		[]byte{0x04}, tstr("conflict\n"),
		[]byte{0x07, 0x50}, changeID, // bytes(16)
		[]byte{0x08, 0x82}, bstr32(c.ConflictTerms[0]), bstr32(c.ConflictTerms[1]),
		[]byte{0x09, 0x83}, tstr("ours"), tstr(""), tstr("theirs"),
	)
}

func TestConflictedCommitMatchesHandAssembledBytes(t *testing.T) {
	got, err := conflicted(t).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if want := handConflicted(t); !bytes.Equal(got, want) {
		t.Fatalf("encoding differs from the spec\n got: %x\nwant: %x", got, want)
	}
}

func TestNewFieldsRoundTrip(t *testing.T) {
	withID := merge(t)
	withID.ChangeID = []byte{0xab}
	terms := merge(t)
	terms.ConflictTerms = conflicted(t).ConflictTerms
	signed := conflicted(t)
	signed.Signature, signed.PublicKey = []byte("sig"), []byte("pub")
	for name, c := range map[string]commit.Commit{"change id": withID, "terms": terms, "terms and labels": conflicted(t), "everything": signed} {
		b, err := c.Encode()
		if err != nil {
			t.Fatalf("%s: Encode: %v", name, err)
		}
		got, err := commit.Decode(b)
		if err != nil {
			t.Fatalf("%s: Decode: %v", name, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Errorf("%s: round trip\n got: %+v\nwant: %+v", name, got, c)
		}
	}
	if c := merge(t); c.Conflicted() || !reflect.DeepEqual(c.Trees(), []key.Key{c.Tree}) {
		t.Errorf("a resolved commit: Conflicted = %v, Trees = %v", c.Conflicted(), c.Trees())
	}
	c := conflicted(t)
	if want := []key.Key{c.Tree, c.ConflictTerms[0], c.ConflictTerms[1]}; !c.Conflicted() || !reflect.DeepEqual(c.Trees(), want) {
		t.Errorf("a conflicted commit: Conflicted = %v, Trees = %v", c.Conflicted(), c.Trees())
	}
}

func TestEncodeAcceptsJJBounds(t *testing.T) {
	many := make([]key.Key, commit.MaxConflictTerms)
	for i := range many {
		many[i] = dirKey(t, key.DirLeaf, uint64(i+1), byte(i))
	}
	cases := map[string]func(*commit.Commit){
		"max terms":        func(c *commit.Commit) { c.ConflictTerms, c.ConflictLabels = many, nil },
		"a term repeats":   func(c *commit.Commit) { c.ConflictTerms[1] = c.ConflictTerms[0] },
		"terms, no labels": func(c *commit.Commit) { c.ConflictLabels = nil },
		"max label":        func(c *commit.Commit) { c.ConflictLabels[1] = strings.Repeat("l", commit.MaxLabelLen) },
		"a long subject": func(c *commit.Commit) {
			c.ConflictLabels[1] = `wqnwkozp 2768b0b9 "` + strings.Repeat("a paragraph on one line ", 200) + `"`
		}, // jj labels carry a description's whole first line
		"one-byte id": func(c *commit.Commit) { c.ChangeID = []byte{0} },
		"max id":      func(c *commit.Commit) { c.ChangeID = make([]byte, commit.MaxChangeIDLen) },
	}
	for name, mutate := range cases {
		c := conflicted(t)
		mutate(&c)
		b, err := c.Encode()
		if err != nil {
			t.Errorf("%s: Encode: %v", name, err)
			continue
		}
		if _, err := commit.Decode(b); err != nil {
			t.Errorf("%s: Decode: %v", name, err)
		}
	}
}

func TestEncodeRejectsInvalidJJFields(t *testing.T) {
	ka, _ := root(t, "a")
	blob, _ := key.NewFromHash(key.Blob, 9, [32]byte{1})
	tooMany := make([]key.Key, commit.MaxConflictTerms+2)
	for i := range tooMany {
		tooMany[i] = dirKey(t, key.DirLeaf, uint64(i+1), byte(i))
	}
	cases := map[string]func(*commit.Commit){
		"one term":              func(c *commit.Commit) { c.ConflictTerms, c.ConflictLabels = c.ConflictTerms[:1], nil },
		"too many terms":        func(c *commit.Commit) { c.ConflictTerms, c.ConflictLabels = tooMany, nil },
		"term is a blob":        func(c *commit.Commit) { c.ConflictTerms[0] = blob },
		"term is a commit":      func(c *commit.Commit) { c.ConflictTerms[1] = ka },
		"non-canonical term":    func(c *commit.Commit) { c.ConflictTerms[0][31] |= 0x08 },
		"labels without terms":  func(c *commit.Commit) { c.ConflictTerms = nil },
		"one label, no terms":   func(c *commit.Commit) { c.ConflictTerms, c.ConflictLabels = nil, []string{"ours"} }, // the count is right: only the rule against labels on a resolved tree rejects it
		"a label too few":       func(c *commit.Commit) { c.ConflictLabels = c.ConflictLabels[:2] },
		"a label too many":      func(c *commit.Commit) { c.ConflictLabels = append(c.ConflictLabels, "x") },
		"all labels empty":      func(c *commit.Commit) { c.ConflictLabels = []string{"", "", ""} },
		"label too long":        func(c *commit.Commit) { c.ConflictLabels[0] = strings.Repeat("l", commit.MaxLabelLen+1) },
		"control char in label": func(c *commit.Commit) { c.ConflictLabels[0] = "ou\nrs" },
		"invalid UTF-8 label":   func(c *commit.Commit) { c.ConflictLabels[2] = "the\xffirs" },
		"change id too long":    func(c *commit.Commit) { c.ChangeID = make([]byte, commit.MaxChangeIDLen+1) },
	}
	for name, mutate := range cases {
		c := conflicted(t)
		c.ConflictTerms, c.ConflictLabels = bytesCloneKeys(c.ConflictTerms), append([]string{}, c.ConflictLabels...)
		mutate(&c)
		if _, err := c.Encode(); err == nil {
			t.Errorf("%s: Encode accepted an invalid commit", name)
		}
		if _, _, err := c.Object(); err == nil {
			t.Errorf("%s: Object accepted an invalid commit", name)
		}
	}
}

func bytesCloneKeys(in []key.Key) []key.Key { return append([]key.Key{}, in...) }

func TestDecodeRejectsNonCanonicalJJFields(t *testing.T) {
	good := handConflicted(t)
	if _, err := commit.Decode(good); err != nil {
		t.Fatalf("baseline must decode: %v", err)
	}
	c := conflicted(t)
	terms := cat([]byte{0x08, 0x82}, bstr32(c.ConflictTerms[0]), bstr32(c.ConflictTerms[1]))
	labels := cat([]byte{0x09, 0x83}, tstr("ours"), tstr(""), tstr("theirs"))
	resolved := handMerge(t)
	grown := func(b []byte, extra ...byte) []byte { // one more map entry
		out := append(bytes.Clone(b), extra...)
		out[0]++
		return out
	}
	cases := map[string][]byte{
		"empty terms":         grown(resolved, 0x08, 0x80),
		"empty change id":     grown(resolved, 0x07, 0x40),
		"empty labels":        replaceOnce(t, good, labels, []byte{0x09, 0x80}),
		"labels before terms": replaceOnce(t, good, cat(terms, labels), cat(labels, terms)),
		"labels, no terms":    func() []byte { b := replaceOnce(t, good, terms, nil); b[0]--; return b }(),
		"one term":            replaceOnce(t, good, terms, cat([]byte{0x08, 0x81}, bstr32(c.ConflictTerms[0]))),
		"term is a commit":    replaceOnce(t, good, bstr32(c.ConflictTerms[1]), bstr32(c.Parents[0])),
	}
	for name, b := range cases {
		if _, err := commit.Decode(b); err == nil {
			t.Errorf("%s: Decode accepted % x", name, b)
		}
	}
}

func TestSignaturePayloadCoversNewFields(t *testing.T) {
	base, err := conflicted(t).SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*commit.Commit){
		"change id": func(c *commit.Commit) { c.ChangeID = []byte{9} },
		"a label":   func(c *commit.Commit) { c.ConflictLabels = []string{"ours", "base", "theirs"} },
		"a term":    func(c *commit.Commit) { c.ConflictTerms = []key.Key{c.ConflictTerms[1], c.ConflictTerms[0]} },
	} {
		c := conflicted(t)
		mutate(&c)
		if got, _ := c.SignaturePayload(); bytes.Equal(got, base) {
			t.Errorf("the signature payload does not cover %s", name)
		}
	}
}

// The key's length is a footprint, as a directory's is: the commit's own
// bytes plus each of its trees. Parents are not counted.
func TestObjectLengthIsTheFootprint(t *testing.T) {
	k, b, err := conflicted(t).Object()
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(len(b)) + 1 + 300 + 70000; k.Length() != want {
		t.Fatalf("key length = %d, want %d: own bytes and every term", k.Length(), want)
	}
	// A parent's length, however large, changes nothing.
	c := conflicted(t)
	huge, _ := key.NewFromHash(key.Commit, 1<<40, [32]byte{7})
	c.Parents = []key.Key{huge}
	k2, b2, err := c.Object()
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(len(b2)) + 1 + 300 + 70000; k2.Length() != want {
		t.Fatalf("key length = %d, want %d: parents must not be counted", k2.Length(), want)
	}
}

func TestObjectRejectsLengthOverflow(t *testing.T) {
	c := conflicted(t)
	c.ConflictLabels = nil
	c.ConflictTerms = []key.Key{dirKey(t, key.DirNode, math.MaxUint64, 0x33), dirKey(t, key.DirLeaf, 5, 0x44)}
	if _, _, err := c.Object(); err == nil {
		t.Fatal("Object accepted trees whose lengths overflow 64 bits")
	}
}

// The second golden vector: conflicted(t), pinned like the first. Tree the
// empty directory; conflict terms key.NewFromHash(DirLeaf, 300, 0x11…) and
// key.NewFromHash(DirNode, 70000, 0x22…); labels "ours", "", "theirs"; change
// id 00 01 … 0f; parent the root commit "a"; author Ann; committer without a
// name, bot@example.com, at Bob's time and zone; message "conflict\n".
// 258 bytes; length field 258+1+300+70000 = 70559.
const (
	goldenConflictedBytes = "a80058206bf3580bff464ed85a9dce79a9f1c21aedb0e981034d1f6a14a0f5a9e6bb0120018158208f2a0d816398cc068c636a2859d576a0706ee8e33cd4673e14e81ac87de9745002a40063416e6e016f616e6e406578616d706c652e636f6d021b17979cfe362a000003187803a40060016f626f74406578616d706c652e636f6d021b17979cfe362a00010339012b0469636f6e666c6963740a0750000102030405060708090a0b0c0d0e0f0882582011111111111111111111111111111111111111111111111111111111112c0121582022222222222222222222222222222222222222222222222222222222701101320983646f7572736066746865697273"
	goldenConflictedKey   = "db31bdde6530514479a6a921f92327794b8bd1d7f61173ade7706d0d9f130152"
)

func TestGoldenVectorConflicted(t *testing.T) {
	k, b, err := conflicted(t).Object()
	if err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]string{
		"bytes": {hex.EncodeToString(b), goldenConflictedBytes},
		"key":   {k.String(), goldenConflictedKey},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s changed!\n got: %s\nwant: %s", name, pair[0], pair[1])
		}
	}
}
