package fstree_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/draganm/rebma/cborx"
	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// counted wraps get and counts the reads of each key.
func counted(get func(key.Key) ([]byte, error)) (func(key.Key) ([]byte, error), map[key.Key]int) {
	reads := map[key.Key]int{}
	return func(k key.Key) ([]byte, error) {
		reads[k]++
		return get(k)
	}, reads
}

// sameAnswer fails the test unless got and gotErr, a DirectoryReader's
// answer, are want and wantErr, the answer of the function of the same name:
// the same value, and an error of the same text that wraps the same
// sentinels.
func sameAnswer(t *testing.T, what string, got any, gotErr error, want any, wantErr error) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %+v, want %+v", what, got, want)
	}
	if (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
		t.Errorf("%s: err = %v, want %v", what, gotErr, wantErr)
	}
	for _, sentinel := range []error{fstree.ErrNotFound, fstree.ErrNotDir} {
		if errors.Is(gotErr, sentinel) != errors.Is(wantErr, sentinel) {
			t.Errorf("%s: errors.Is(err, %q) = %v, want %v", what, sentinel, errors.Is(gotErr, sentinel), errors.Is(wantErr, sentinel))
		}
	}
}

// sameLookup, sameResolvePath and sameResolveEntry hold one answer of r to the
// function's, through get, and return r's error.
func sameLookup(t *testing.T, r *fstree.DirectoryReader, dir key.Key, name string, get func(key.Key) ([]byte, error)) error {
	t.Helper()
	got, gotErr := r.LookupEntry(dir, []byte(name))
	want, wantErr := fstree.LookupEntry(dir, []byte(name), get)
	sameAnswer(t, fmt.Sprintf("LookupEntry(%q)", name), got, gotErr, want, wantErr)
	return gotErr
}

func sameResolvePath(t *testing.T, r *fstree.DirectoryReader, root key.Key, path string, get func(key.Key) ([]byte, error)) error {
	t.Helper()
	got, gotErr := r.ResolvePath(root, path)
	want, wantErr := fstree.ResolvePath(root, path, get)
	sameAnswer(t, fmt.Sprintf("ResolvePath(%q)", path), got, gotErr, want, wantErr)
	return gotErr
}

func sameResolveEntry(t *testing.T, r *fstree.DirectoryReader, root key.Key, path string, get func(key.Key) ([]byte, error)) error {
	t.Helper()
	got, gotErr := r.ResolveEntry(root, path)
	want, wantErr := fstree.ResolveEntry(root, path, get)
	sameAnswer(t, fmt.Sprintf("ResolveEntry(%q)", path), got, gotErr, want, wantErr)
	return gotErr
}

func TestDirectoryReader_ReusesDecodesAndMatchesSingleLookups(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	get, reads := counted(store.get)
	r := fstree.NewDirectoryReader(get)
	for range 2 {
		for _, name := range []string{"e00000", "e00001", "e00499", "e00998", "e00999", "", "a", "zzz"} {
			sameLookup(t, r, root, name, store.get)
		}
		for _, path := range []string{"", ".", "/", "./", "e00000", "../e00000", "absent"} {
			sameResolvePath(t, r, root, path, store.get)
		}
	}
	if len(reads) < 2 {
		t.Fatalf("the reader read %d objects, want the several levels of a big directory", len(reads))
	}
	for k, n := range reads {
		if n != 1 {
			t.Errorf("%s was read %d times, want once", k, n)
		}
	}
}

func TestDirectoryReader_EvictionPreservesLookupResults(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	for _, capacity := range []int{-1, 0, 1, 2, 3, 64} {
		r := fstree.NewDirectoryReader(store.get, fstree.WithCapacity(capacity))
		for range 2 {
			for i := range 1000 {
				name := fmt.Sprintf("e%05d", i)
				if err := sameLookup(t, r, root, name, store.get); err != nil {
					t.Fatalf("capacity %d: LookupEntry(%s): %v", capacity, name, err)
				}
				if held, _ := r.Held(); held > max(capacity, 1) {
					t.Fatalf("capacity %d: the reader holds %d directory objects", capacity, held)
				}
			}
		}
	}
}

// Five directories of one leaf each, so that one lookup is one directory
// object, and the reads show which of them the reader kept.
func TestDirectoryReader_DropsTheLeastRecentlyUsedHalf(t *testing.T) {
	store := memStore{}
	dirs := map[string]key.Key{}
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		tree, _ := oneFileTree(t, name, name)
		if err := store.emit(tree); err != nil {
			t.Fatal(err)
		}
		dirs[name] = tree.Key
	}
	get, reads := counted(store.get)
	r := fstree.NewDirectoryReader(get, fstree.WithCapacity(4))
	lookup := func(names ...string) {
		t.Helper()
		for _, name := range names {
			if _, err := r.LookupEntry(dirs[name], []byte(name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	readsOf := func() map[string]int {
		got := map[string]int{}
		for name, k := range dirs {
			got[name] = reads[k]
		}
		return got
	}

	lookup("a", "b", "c", "d") // full
	lookup("a")                // from oldest to newest: b c d a
	lookup("e")                // drops b and c, the older half
	lookup("a", "d", "e")
	if got, want := readsOf(), map[string]int{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reads = %v, want %v: the newer half and the newcomer are held", got, want)
	}
	lookup("b", "c")
	if got, want := readsOf(), map[string]int{"a": 1, "b": 2, "c": 2, "d": 1, "e": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reads = %v, want %v: the older half was dropped", got, want)
	}
}

func TestDirectoryReader_PassesThroughACommit(t *testing.T) {
	v := vendoredTree(t)
	plain := mapGetter(v.all...)
	get, reads := counted(plain)
	r := fstree.NewDirectoryReader(get)
	for range 2 {
		sameLookup(t, r, v.commit.Key, "README", plain)
		for _, path := range []string{"vendor", "vendor/sub", "vendor/sub/file", "vendor/absent", ""} {
			sameResolveEntry(t, r, v.top.Key, path, plain)
			sameResolvePath(t, r, v.top.Key, path, plain)
		}
	}
	want := map[key.Key]int{v.top.Key: 1, v.commit.Key: 1, v.inner.Key: 1, v.sub.Key: 1}
	if !reflect.DeepEqual(reads, want) {
		t.Errorf("reads = %v, want one of the commit and of each directory: %v", reads, want)
	}
}

func TestDirectoryReader_BoundsItsCommits(t *testing.T) {
	tree, blob := oneFileTree(t, "f", "x")
	objs := []fstree.Object{tree, blob}
	var commits []fstree.Object
	for i := range 10 {
		c := commitObj(t, fmt.Sprintf("c%d", i), tree.Key)
		commits = append(commits, c)
		objs = append(objs, c)
	}
	get := mapGetter(objs...)
	for _, capacity := range []int{1, 3} {
		r := fstree.NewDirectoryReader(get, fstree.WithCapacity(capacity))
		for _, c := range commits {
			if err := sameLookup(t, r, c.Key, "f", get); err != nil {
				t.Fatalf("capacity %d: LookupEntry(commit, f): %v", capacity, err)
			}
			if _, held := r.Held(); held > capacity {
				t.Fatalf("capacity %d: the reader holds %d commits", capacity, held)
			}
		}
	}
}

func TestDirectoryReader_DoesNotCacheFailedReadsOrDecodes(t *testing.T) {
	body := []byte{0xff} // not a DirLeaf
	k, err := key.New(key.DirLeaf, uint64(len(body)), body)
	if err != nil {
		t.Fatal(err)
	}
	malformed := func(key.Key) ([]byte, error) { return body, nil }
	get, reads := counted(malformed)
	r := fstree.NewDirectoryReader(get)
	for range 2 {
		if err := sameLookup(t, r, k, "entry", malformed); err == nil {
			t.Fatal("LookupEntry decoded a malformed DirLeaf")
		}
	}
	if reads[k] != 2 {
		t.Errorf("the malformed object was read %d times in 2 lookups, want 2", reads[k])
	}

	errMissing := errors.New("missing")
	missing := func(key.Key) ([]byte, error) { return nil, errMissing }
	r = fstree.NewDirectoryReader(missing)
	if err := sameLookup(t, r, k, "entry", missing); !errors.Is(err, errMissing) {
		t.Errorf("err = %v, want it to wrap the getter's", err)
	}
}

// The errors that the scenarios above do not reach: a key that is not a
// directory's, as the root and inside a directory's index; a path through a
// file; a commit that is not there.
func TestDirectoryReader_ErrorsAreTheFunctions(t *testing.T) {
	v := vendoredTree(t)
	node, err := fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("z"), ChildKey: v.commit.Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	get := mapGetter(append(v.all, node)...)
	gone := mapGetter(without(v.all, v.commit.Key)...)
	r, rGone := fstree.NewDirectoryReader(get), fstree.NewDirectoryReader(gone)
	for range 2 {
		for what, err := range map[string]error{
			"a blob for a directory":            sameLookup(t, r, v.blobM.Key, "x", get),
			"a commit as the child of a node":   sameLookup(t, r, node.Key, "README", get),
			"ResolveEntry through a file":       sameResolveEntry(t, r, v.top.Key, "main.go/x", get),
			"ResolvePath through a file":        sameResolvePath(t, r, v.top.Key, "main.go/x", get),
			"an entry's commit that is missing": sameResolveEntry(t, rGone, v.top.Key, "vendor/sub", gone),
		} {
			if err == nil {
				t.Errorf("%s: no error", what)
			}
		}
	}
}

// The functions hand out entries decoded for that one call, which the caller
// may change. The reader's come from what it holds, and are to be as free.
func TestDirectoryReader_ReturnedEntriesAreTheCallersOwn(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	spilled, err := fstree.EncodeXattrSet(map[string][]byte{"user.a": []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	leaf := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("dev"), Mode: 0o020644, Rdev: []uint64{1, 2}},
		{Name: []byte("file"), Mode: 0o100644, ContentKey: blob.Key[:], XattrsIn: cborx.EncodeXattrs(map[string][]byte{"user.x": []byte("y")})},
		{Name: []byte("link"), Mode: 0o120777, LinkTarget: []byte("file"), XattrsKey: spilled.Key[:]},
	})
	get := mapGetter(leaf)
	r := fstree.NewDirectoryReader(get)

	// scribble changes every element of every slice e holds.
	scribbled := map[string]bool{}
	scribble := func(e *fstree.Entry) {
		v := reflect.ValueOf(e).Elem()
		for i := range v.NumField() {
			f := v.Field(i)
			if f.Kind() != reflect.Slice {
				continue
			}
			for j := range f.Len() {
				f.Index(j).SetUint(f.Index(j).Uint() + 1)
			}
			if f.Len() > 0 {
				scribbled[v.Type().Field(i).Name] = true
			}
		}
	}

	for _, name := range []string{"dev", "file", "link"} {
		want, err := fstree.LookupEntry(leaf.Key, []byte(name), get)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.LookupEntry(leaf.Key, []byte(name))
		if err != nil {
			t.Fatal(err)
		}
		scribble(&got)
		if got, err := r.LookupEntry(leaf.Key, []byte(name)); err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("LookupEntry(%s) after its last answer was changed = %+v, %v; want %+v", name, got, err, want)
		}
		resolved, err := r.ResolveEntry(leaf.Key, name)
		if err != nil {
			t.Fatal(err)
		}
		scribble(resolved)
		if got, err := r.ResolveEntry(leaf.Key, name); err != nil || !reflect.DeepEqual(got, &want) {
			t.Errorf("ResolveEntry(%s) after its last answer was changed = %+v, %v; want %+v", name, got, err, want)
		}
	}

	// Every slice an Entry holds was put to the test: one added to Entry
	// later has to be given to a fixture entry above.
	typ := reflect.TypeFor[fstree.Entry]()
	for i := range typ.NumField() {
		if f := typ.Field(i); f.Type.Kind() == reflect.Slice && !scribbled[f.Name] {
			t.Errorf("no fixture entry has a non-empty %s", f.Name)
		}
	}
}

func TestDirectoryReader_WriteContent(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	tree, _ := oneFileTree(t, "f", "hello")
	get := mapGetter(blob, tree)
	r := fstree.NewDirectoryReader(get)

	var out bytes.Buffer
	if err := r.WriteContent(&out, blob.Key); err != nil || out.String() != "hello" {
		t.Errorf("WriteContent(blob) wrote %q, %v; want \"hello\"", out.String(), err)
	}
	gotErr := r.WriteContent(&out, tree.Key)
	wantErr := fstree.WriteContent(&out, tree.Key, get)
	if gotErr == nil || gotErr.Error() != wantErr.Error() {
		t.Errorf("WriteContent(directory): err = %v, want %v", gotErr, wantErr)
	}
}

// benchTree builds a tree of 9,617 directories, three levels of them under
// the root with two files in each of the lowest, chunked as ingest chunks
// directories. It returns every path of the tree, 28,048 of them, each
// directory before what it holds.
func benchTree(b *testing.B) (get func(key.Key) ([]byte, error), root key.Key, paths []string) {
	b.Helper()
	const top, mid, low, files = 16, 24, 24, 2
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		b.Fatal(err)
	}
	mtime := int64(0) // differs in every entry, so that no two directories are one object
	build := func(entries []fstree.Entry) key.Key {
		db := fstree.NewDirBuilder(chunkers.NewItemChunker(7)) // ingest.DefaultItemBits
		for _, e := range entries {
			mtime++
			e.Mtime = mtime
			if err := db.AddEntry(store.emit, e); err != nil {
				b.Fatal(err)
			}
		}
		k, err := db.Finish(store.emit)
		if err != nil {
			b.Fatal(err)
		}
		return k
	}
	dir := func(name string, k key.Key) fstree.Entry {
		return fstree.Entry{Name: []byte(name), Mode: 0o040755, ContentKey: k[:]}
	}

	var tops []fstree.Entry
	for i := range top {
		tName := fmt.Sprintf("t%02d", i)
		paths = append(paths, tName)
		var mids []fstree.Entry
		for j := range mid {
			mName := fmt.Sprintf("m%02d", j)
			paths = append(paths, tName+"/"+mName)
			var lows []fstree.Entry
			for k := range low {
				lName := fmt.Sprintf("l%02d", k)
				paths = append(paths, tName+"/"+mName+"/"+lName)
				var leaves []fstree.Entry
				for l := range files {
					fName := fmt.Sprintf("f%d", l)
					paths = append(paths, tName+"/"+mName+"/"+lName+"/"+fName)
					leaves = append(leaves, fstree.Entry{Name: []byte(fName), Mode: 0o100644, ContentKey: blob.Key[:]})
				}
				lows = append(lows, dir(lName, build(leaves)))
			}
			mids = append(mids, dir(mName, build(lows)))
		}
		tops = append(tops, dir(tName, build(mids)))
	}
	return store.get, build(tops), paths
}

// BenchmarkResolveEntry_EveryPath stats every path of one tree: with the
// function, which decodes each directory object on a path at every call, and
// with a DirectoryReader made for the pass, at several capacities. Random
// order is the worst case for a bounded reader; the order of a walk is the
// usual one.
func BenchmarkResolveEntry_EveryPath(b *testing.B) {
	get, root, walk := benchTree(b)
	random := slices.Clone(walk)
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(random), func(i, j int) { random[i], random[j] = random[j], random[i] })

	for _, order := range []struct {
		name  string
		paths []string
	}{{"random", random}, {"walk", walk}} {
		perPath := func(b *testing.B) {
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(order.paths)), "ns/path")
		}
		b.Run("order="+order.name+"/function", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, p := range order.paths {
					if _, err := fstree.ResolveEntry(root, p, get); err != nil {
						b.Fatal(err)
					}
				}
			}
			perPath(b)
		})
		for _, capacity := range []int{64, 512, fstree.DefaultDirectoryReaderCapacity, 16384} {
			b.Run(fmt.Sprintf("order=%s/reader=%d", order.name, capacity), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					r := fstree.NewDirectoryReader(get, fstree.WithCapacity(capacity))
					for _, p := range order.paths {
						if _, err := r.ResolveEntry(root, p); err != nil {
							b.Fatal(err)
						}
					}
				}
				perPath(b)
			})
		}
	}
}
