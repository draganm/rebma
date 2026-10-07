package fstree

import (
	"fmt"

	"github.com/draganm/rebma/cborx"
	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
)

// encMode is the RFC 8949 section 4.2 core-deterministic CBOR encoder shared by
// every object encoder. Built once at init. NilContainerAsEmpty makes a nil or
// empty slice encode as an empty array (0x80) rather than null (0xf6) — required
// so an empty directory's DirLeaf is the canonical empty array.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("fstree: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Entry is a single directory entry, encoded as a canonical CBOR map with
// integer keys 0-9 (architecture/fstree.md, DirLeaf). Fields are declared in
// ascending key order so the encoding is canonical. Required keys (0-4) are
// never omitempty because uid/gid/mtime/mode are legitimately zero. The payload
// keys 5-9 are mutually constrained by the caller per the entry's type.
type Entry struct {
	Name       []byte          `cbor:"0,keyasint"`
	Mode       uint64          `cbor:"1,keyasint"` // raw POSIX st_mode (type + perms)
	UID        uint64          `cbor:"2,keyasint"`
	GID        uint64          `cbor:"3,keyasint"`
	Mtime      int64           `cbor:"4,keyasint"`           // ns since the Unix epoch (may be negative)
	ContentKey []byte          `cbor:"5,keyasint,omitempty"` // S_IFREG / S_IFDIR
	LinkTarget []byte          `cbor:"6,keyasint,omitempty"` // S_IFLNK
	Rdev       []uint64        `cbor:"7,keyasint,omitempty"` // S_IFCHR / S_IFBLK: [major, minor]
	XattrsIn   cbor.RawMessage `cbor:"8,keyasint,omitempty"` // inline xattrs (pre-encoded bstr map)
	XattrsKey  []byte          `cbor:"9,keyasint,omitempty"` // spilled XattrSet key
}

// DirPair is one [sepName, childKey] element of a DirNode, encoded as a 2-element
// CBOR array of byte strings.
type DirPair struct {
	_        struct{} `cbor:",toarray"`
	SepName  []byte
	ChildKey []byte
}

// EncodeBlob wraps raw file-content bytes as a Blob object (no CBOR framing).
func EncodeBlob(data []byte) (Object, error) {
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: data}, nil
}

// EncodeFileNode encodes a file index node: a CBOR array of child keys. Its
// length field is the sum of child content sizes (excludes its own bytes).
func EncodeFileNode(children []key.Key) (Object, error) {
	arr := make([][]byte, len(children))
	var sum uint64
	for i, c := range children {
		arr[i] = c[:]
		sum += c.Length()
	}
	b, err := encMode.Marshal(arr)
	if err != nil {
		return Object{}, err
	}
	k, err := key.New(key.FileNode, sum, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeDirLeaf encodes a run of directory entries (already sorted by name) as a
// CBOR array of entry maps. Its length field is its own serialized bytes plus
// the content-key length of each regular-file/directory entry plus the
// xattrs-key length of each entry whose xattrs were spilled to an XattrSet.
func EncodeDirLeaf(entries []Entry) (Object, error) {
	b, err := encMode.Marshal(entries)
	if err != nil {
		return Object{}, err
	}
	var sub uint64
	for _, e := range entries {
		if len(e.ContentKey) == key.Size {
			ck, err := key.Parse(e.ContentKey)
			if err != nil {
				return Object{}, fmt.Errorf("entry %q content key: %w", e.Name, err)
			}
			sub += ck.Length()
		}
		if len(e.XattrsKey) == key.Size {
			xk, err := key.Parse(e.XattrsKey)
			if err != nil {
				return Object{}, fmt.Errorf("entry %q xattrs key: %w", e.Name, err)
			}
			sub += xk.Length()
		}
	}
	k, err := key.New(key.DirLeaf, uint64(len(b))+sub, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeDirNode encodes a directory index node as a CBOR array of
// [sepName, childKey] pairs (sorted by sepName). Its length field is its own
// serialized bytes plus the cumulative length of every child.
func EncodeDirNode(pairs []DirPair) (Object, error) {
	b, err := encMode.Marshal(pairs)
	if err != nil {
		return Object{}, err
	}
	var sub uint64
	for _, p := range pairs {
		ck, err := key.Parse(p.ChildKey)
		if err != nil {
			return Object{}, fmt.Errorf("dir node child key: %w", err)
		}
		sub += ck.Length()
	}
	k, err := key.New(key.DirNode, uint64(len(b))+sub, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeXattrSet encodes a spilled extended-attribute set. Its length field is
// its own serialized byte length.
func EncodeXattrSet(m map[string][]byte) (Object, error) {
	b := cborx.EncodeXattrs(m)
	k, err := key.New(key.XattrSet, uint64(len(b)), b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}
