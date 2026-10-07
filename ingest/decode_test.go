package ingest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
	"golang.org/x/sys/unix"
)

// store is the decoded object set: key hex -> object bytes.
type store map[string][]byte

// loadStore builds the CAS object set for dir via the sequential driver,
// keyed by key hex, and returns it with the root key.
func loadStore(t *testing.T, dir string) (store, key.Key) {
	t.Helper()
	root, objs := collectSequential(t, dir, nil, chunkers.NewItemChunker(7), nil, 256)
	s := store{}
	for k, v := range objs {
		s[k.String()] = v
	}
	return s, root
}

// fileContent reassembles a file's bytes from its content key.
func (s store) fileContent(t *testing.T, k key.Key) []byte {
	t.Helper()
	data := s[k.String()]
	switch k.Type() {
	case key.Blob:
		return data
	case key.FileNode:
		var childRaw [][]byte
		if err := cbor.Unmarshal(data, &childRaw); err != nil {
			t.Fatal(err)
		}
		var out []byte
		for _, cr := range childRaw {
			ck, err := key.Parse(cr)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, s.fileContent(t, ck)...)
		}
		return out
	default:
		t.Fatalf("not a file content key: %v", k.Type())
		return nil
	}
}

// entryMap is a decoded DirLeaf entry.
type entryMap struct {
	Name       []byte `cbor:"0,keyasint"`
	Mode       uint64 `cbor:"1,keyasint"`
	ContentKey []byte `cbor:"5,keyasint,omitempty"`
	LinkTarget []byte `cbor:"6,keyasint,omitempty"`
}

// listDir collects all entries under a directory content key, descending
// DirNodes and DirLeaves.
func (s store) listDir(t *testing.T, k key.Key) []entryMap {
	t.Helper()
	data := s[k.String()]
	switch k.Type() {
	case key.DirLeaf:
		var ents []entryMap
		if err := cbor.Unmarshal(data, &ents); err != nil {
			t.Fatal(err)
		}
		return ents
	case key.DirNode:
		var pairs [][]cbor.RawMessage
		if err := cbor.Unmarshal(data, &pairs); err != nil {
			t.Fatal(err)
		}
		var out []entryMap
		for _, p := range pairs {
			var childKeyBytes []byte
			if err := cbor.Unmarshal(p[1], &childKeyBytes); err != nil {
				t.Fatal(err)
			}
			ck, err := key.Parse(childKeyBytes)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, s.listDir(t, ck)...)
		}
		return out
	default:
		t.Fatalf("not a directory key: %v", k.Type())
		return nil
	}
}

func TestEndToEnd_StructureRoundTrips(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("0123456789"), 200000) // ~2 MiB -> multi-chunk file
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "small.txt"), []byte("tiny"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("big.bin", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	s, root := loadStore(t, dir)

	ents := s.listDir(t, root)
	byName := map[string]entryMap{}
	for _, e := range ents {
		byName[string(e.Name)] = e
	}

	if _, ok := byName["big.bin"]; !ok {
		t.Fatal("big.bin missing")
	}
	bigKey, err := key.Parse(byName["big.bin"].ContentKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.fileContent(t, bigKey); !bytes.Equal(got, big) {
		t.Errorf("big.bin content mismatch: got %d bytes, want %d", len(got), len(big))
	}
	if bigKey.Length() != uint64(len(big)) {
		t.Errorf("big.bin content key length = %d, want %d", bigKey.Length(), len(big))
	}

	small := byName["small.txt"]
	if small.Mode&0o777 != 0o600 {
		t.Errorf("small.txt perms = %o, want 600", small.Mode&0o777)
	}

	link := byName["link"]
	if link.Mode&unix.S_IFMT != unix.S_IFLNK {
		t.Errorf("link type = %o, want symlink", link.Mode&unix.S_IFMT)
	}
	if string(link.LinkTarget) != "big.bin" {
		t.Errorf("link target = %q, want big.bin", link.LinkTarget)
	}
}
