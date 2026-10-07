package tarexport_test

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/tarexport"
)

// buildStore ingests three blobs + a single-leaf directory referencing two
// regular files, returning the store and the directory root key. It encodes the
// objects directly with fstree to avoid depending on the CLI.
func TestWrite_RegularFilesAndDir(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	put := func(o fstree.Object) {
		t.Helper()
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	// Two files: "a" -> "alpha", "b" -> "beta" (each a single Blob).
	ablob, _ := fstree.EncodeBlob([]byte("alpha"))
	bblob, _ := fstree.EncodeBlob([]byte("beta"))
	put(ablob)
	put(bblob)

	// A directory leaf with two regular-file entries (mode 0o100644).
	entries := []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: ablob.Key[:]},
		{Name: []byte("b"), Mode: 0o100644, Mtime: 2, ContentKey: bblob.Key[:]},
	}
	leaf, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	put(leaf)

	var buf bytes.Buffer
	if err := tarexport.Write(&buf, leaf.Key, store.Get); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := map[string]string{}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if got["a"] != "alpha" || got["b"] != "beta" {
		t.Fatalf("tar contents = %v, want a=alpha b=beta", got)
	}
}

func TestWrite_RejectsNonDirectoryRoot(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blob, _ := fstree.EncodeBlob([]byte("x"))
	if err := store.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tarexport.Write(&buf, blob.Key, store.Get); err == nil {
		t.Fatalf("expected error exporting a non-directory root (type %v)", key.Blob)
	}
}

func TestWrite_NestedDir(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	put := func(o fstree.Object) {
		t.Helper()
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	fblob, _ := fstree.EncodeBlob([]byte("deep"))
	put(fblob)
	child, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f.txt"), Mode: 0o100644, Mtime: 1, ContentKey: fblob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	put(child)
	root, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("d"), Mode: 0o040755, Mtime: 2, ContentKey: child.Key[:]}, // S_IFDIR
	})
	if err != nil {
		t.Fatal(err)
	}
	put(root)

	var buf bytes.Buffer
	if err := tarexport.Write(&buf, root.Key, store.Get); err != nil {
		t.Fatalf("Write: %v", err)
	}

	names := map[string]string{}
	types := map[string]byte{}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		names[h.Name] = string(data)
		types[h.Name] = h.Typeflag
	}
	if types["d/"] != tar.TypeDir {
		t.Errorf("expected a TypeDir header for d/, got types=%v", types)
	}
	if names["d/f.txt"] != "deep" {
		t.Errorf("nested file content = %q, want deep (names=%v)", names["d/f.txt"], names)
	}
}

func TestWrite_RejectsUnsafeEntryName(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blob, _ := fstree.EncodeBlob([]byte("x"))
	if err := store.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte(".."), Mode: 0o100644, Mtime: 1, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(leaf.Key, leaf.Bytes); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tarexport.Write(&buf, leaf.Key, store.Get); err == nil {
		t.Fatalf("expected error for entry named %q", "..")
	}
}

// untar reads a tar stream into name -> content and name -> type flag.
func untar(t *testing.T, r io.Reader) (map[string]string, map[string]byte) {
	t.Helper()
	contents, types := map[string]string{}, map[string]byte{}
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return contents, types
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		contents[h.Name], types[h.Name] = string(data), h.Typeflag
	}
}

// A commit is skipped over: where a directory entry holds one, and where one
// is the root, the archive contains the commit's tree as a plain directory.
func TestWrite_ReadsThroughACommit(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	put := func(o fstree.Object) fstree.Object {
		t.Helper()
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		return o
	}
	leaf := func(entries ...fstree.Entry) fstree.Object {
		t.Helper()
		o, err := fstree.EncodeDirLeaf(entries)
		if err != nil {
			t.Fatal(err)
		}
		return put(o)
	}
	blob := func(s string) fstree.Object {
		t.Helper()
		o, err := fstree.EncodeBlob([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		return put(o)
	}
	ck := func(o fstree.Object) []byte { return o.Key[:] }
	sub := leaf(fstree.Entry{Name: []byte("file"), Mode: 0o100644, ContentKey: ck(blob("inner file"))})
	inner := leaf(
		fstree.Entry{Name: []byte("README"), Mode: 0o100644, ContentKey: ck(blob("readme"))},
		fstree.Entry{Name: []byte("sub"), Mode: 0o040755, ContentKey: sub.Key[:]},
	)
	id := commit.Identity{Name: "Ann", When: 1}
	vendored, vendoredBytes, err := commit.Commit{Tree: inner.Key, Author: id, Committer: id, Message: "vendored"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(vendored, vendoredBytes); err != nil {
		t.Fatal(err)
	}
	top := leaf(
		fstree.Entry{Name: []byte("main.go"), Mode: 0o100644, ContentKey: ck(blob("package main"))},
		fstree.Entry{Name: []byte("vendor"), Mode: 0o040755, ContentKey: vendored[:]}, // a directory entry holding a commit
	)

	var buf bytes.Buffer
	if err := tarexport.Write(&buf, top.Key, store.Get); err != nil {
		t.Fatalf("Write(a tree that holds a commit): %v", err)
	}
	contents, types := untar(t, &buf)
	if types["vendor/"] != tar.TypeDir {
		t.Errorf("vendor/ has type %q, want a directory", types["vendor/"])
	}
	for name, want := range map[string]string{"main.go": "package main", "vendor/README": "readme", "vendor/sub/file": "inner file"} {
		if contents[name] != want {
			t.Errorf("%s = %q, want %q (archive: %v)", name, contents[name], want, contents)
		}
	}

	buf.Reset()
	if err := tarexport.Write(&buf, vendored, store.Get); err != nil {
		t.Fatalf("Write(a commit): %v", err)
	}
	contents, _ = untar(t, &buf)
	if contents["README"] != "readme" || contents["sub/file"] != "inner file" {
		t.Errorf("archive of a commit = %v, want its tree", contents)
	}
}
