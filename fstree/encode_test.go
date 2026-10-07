package fstree

import (
	"testing"

	"github.com/draganm/rebma/cborx"
	"github.com/draganm/rebma/key"
)

func mustBlob(t *testing.T, data []byte) Object {
	t.Helper()
	o, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestEncodeBlob_LengthIsByteCount(t *testing.T) {
	o := mustBlob(t, []byte("hello"))
	if o.Key.Type() != key.Blob {
		t.Errorf("type = %v, want Blob", o.Key.Type())
	}
	if o.Key.Length() != 5 {
		t.Errorf("length = %d, want 5", o.Key.Length())
	}
}

func TestEncodeFileNode_LengthIsSumOfChildren(t *testing.T) {
	a := mustBlob(t, make([]byte, 100))
	b := mustBlob(t, make([]byte, 250))
	o, err := EncodeFileNode([]key.Key{a.Key, b.Key})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.FileNode {
		t.Errorf("type = %v, want FileNode", o.Key.Type())
	}
	if o.Key.Length() != 350 { // excludes the FileNode's own bytes
		t.Errorf("length = %d, want 350", o.Key.Length())
	}
}

func TestEncodeDirLeaf_LengthIsOwnBytesPlusContentKeys(t *testing.T) {
	child := mustBlob(t, make([]byte, 1000)) // a file with content size 1000
	e := Entry{
		Name:       []byte("f"),
		Mode:       0o100644,
		UID:        0,
		GID:        0,
		Mtime:      0,
		ContentKey: child.Key[:],
	}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.DirLeaf {
		t.Errorf("type = %v, want DirLeaf", o.Key.Type())
	}
	want := uint64(len(o.Bytes)) + 1000
	if o.Key.Length() != want {
		t.Errorf("length = %d, want %d (ownbytes %d + 1000)", o.Key.Length(), want, len(o.Bytes))
	}
}

func TestEncodeDirLeaf_SymlinkAddsOnlyOwnBytes(t *testing.T) {
	e := Entry{Name: []byte("l"), Mode: 0o120777, LinkTarget: []byte("target/path")}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Length() != uint64(len(o.Bytes)) {
		t.Errorf("length = %d, want own bytes %d", o.Key.Length(), len(o.Bytes))
	}
}

func TestEncodeDirNode_LengthIsOwnBytesPlusChildren(t *testing.T) {
	// Two DirLeaf children, each with a known length field.
	c1, _ := EncodeDirLeaf([]Entry{{Name: []byte("a"), Mode: 0o40755}})
	c2, _ := EncodeDirLeaf([]Entry{{Name: []byte("b"), Mode: 0o40755}})
	o, err := EncodeDirNode([]DirPair{
		{SepName: []byte("a"), ChildKey: c1.Key[:]},
		{SepName: []byte("b"), ChildKey: c2.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(len(o.Bytes)) + c1.Key.Length() + c2.Key.Length()
	if o.Key.Length() != want {
		t.Errorf("length = %d, want %d", o.Key.Length(), want)
	}
}

func TestEncodeXattrSet_LengthIsOwnBytes(t *testing.T) {
	m := map[string][]byte{"user.a": []byte("v")}
	o, err := EncodeXattrSet(m)
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.XattrSet {
		t.Errorf("type = %v, want XattrSet", o.Key.Type())
	}
	if o.Key.Length() != uint64(len(o.Bytes)) {
		t.Errorf("length = %d, want %d", o.Key.Length(), len(o.Bytes))
	}
	if string(o.Bytes) != string(cborx.EncodeXattrs(m)) {
		t.Errorf("XattrSet body must equal EncodeXattrs output")
	}
}

func TestEncodeDirLeaf_InlineXattrsEmbeddedVerbatim(t *testing.T) {
	inline := cborx.EncodeXattrs(map[string][]byte{"user.x": []byte("y")})
	e := Entry{Name: []byte("f"), Mode: 0o100644, XattrsIn: inline}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	// The inline xattr bytes must appear verbatim somewhere in the leaf encoding.
	if !contains(o.Bytes, inline) {
		t.Errorf("inline xattrs not embedded verbatim")
	}
}

func contains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
outer:
	for i := 0; i+len(needle) <= len(haystack); i++ {
		for j := range needle {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return true
	}
	return false
}

func TestEncoders_Deterministic(t *testing.T) {
	e := Entry{Name: []byte("z"), Mode: 0o100644, UID: 1000, GID: 1000, Mtime: -5}
	a, _ := EncodeDirLeaf([]Entry{e})
	b, _ := EncodeDirLeaf([]Entry{e})
	if a.Key != b.Key {
		t.Errorf("DirLeaf encoding not deterministic: %s vs %s", a.Key, b.Key)
	}
}

func TestEncodeDirLeaf_EmptyIsCanonicalEmptyArray(t *testing.T) {
	o, err := EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Bytes) != 1 || o.Bytes[0] != 0x80 {
		t.Errorf("empty DirLeaf bytes = %x, want 80", o.Bytes)
	}
}
