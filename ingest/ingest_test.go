package ingest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/draganm/rebma/amberignore"
	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
)

// collectSequential builds the tree at dir with the sequential driver and returns
// the root plus a map of every emitted object's key to its bytes. It is the
// reference oracle the parallel build is checked against.
func collectSequential(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	emit := func(o fstree.Object) error {
		objs[o.Key] = append([]byte(nil), o.Bytes...)
		return nil
	}
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}
	root, err := d.buildDir(dir, ign, emit)
	if err != nil {
		t.Fatalf("sequential build: %v", err)
	}
	return root, objs
}

// dirBuildRoot returns the buildRoot closure that objects expects for a
// concurrent directory build (the production wiring used by Objects).
func dirBuildRoot(dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax, jobs int, p Progress) func(fstree.Emit) (key.Key, error) {
	if jobs < 1 {
		jobs = 1
	}
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax, progress: p}
	return func(emit fstree.Emit) (key.Key, error) {
		b := &pbuilder{d: d, emit: emit, sem: make(chan struct{}, jobs)}
		return b.buildDir(dir, ign, emit)
	}
}

// collectParallel drains objects into a key->bytes map and returns the root.
func collectParallel(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax, jobs int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	var root key.Key
	for o, err := range objects(dirBuildRoot(dir, ign, ic, byteOpts, xattrInlineMax, jobs, nil), jobs*2, &root) {
		if err != nil {
			t.Fatalf("parallel build: %v", err)
		}
		objs[o.Key] = append([]byte(nil), o.Bytes...)
	}
	return root, objs
}

func assertSameObjects(t *testing.T, want, got map[key.Key][]byte) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("object count: want %d, got %d", len(want), len(got))
	}
	for k, wb := range want {
		gb, ok := got[k]
		if !ok {
			t.Errorf("missing object %s", k)
			continue
		}
		if !bytes.Equal(wb, gb) {
			t.Errorf("object %s bytes differ", k)
		}
	}
}

func TestObjects_ParityWithSequential(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, nil, ic, nil, 256)
	parRoot, parObjs := collectParallel(t, dir, nil, ic, nil, 256, 4)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	assertSameObjects(t, seqObjs, parObjs)
}

func TestObjects_ParallelParityDeepTree(t *testing.T) {
	dir := t.TempDir()
	writeDeepTree(t, dir)
	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, nil, ic, nil, 256)
	jobs := max(runtime.NumCPU(), 4)
	parRoot, parObjs := collectParallel(t, dir, nil, ic, nil, 256, jobs)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	if len(seqObjs) < 50 {
		t.Fatalf("deep tree produced only %d objects; expected a large fan-out", len(seqObjs))
	}
	assertSameObjects(t, seqObjs, parObjs)
}

// TestObjects_RootSetAfterDrain checks the public API: the stream yields every
// object, and the root pointer is set once the stream completes.
func TestObjects_RootSetAfterDrain(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	seq, root, err := Objects(dir, Opts{Jobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[key.Key]bool{}
	for o, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		seen[o.Key] = true
	}
	if (*root == key.Key{}) {
		t.Fatal("root not set after draining the stream")
	}
	if !seen[*root] {
		t.Errorf("stream does not contain the root object %s", root)
	}
	wantRoot, _ := collectSequential(t, dir, nil, chunkers.NewItemChunker(DefaultItemBits), nil, DefaultXattrInlineMax)
	if *root != wantRoot {
		t.Errorf("root %s != sequential oracle root %s", root, wantRoot)
	}
}

// TestObjects_File builds a single regular file and checks the root is a
// file-content key (Blob or FileNode), not a directory.
func TestObjects_File(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(f, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	seq, root, err := Objects(f, Opts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
	}
	if typ := root.Type(); typ != key.Blob && typ != key.FileNode {
		t.Fatalf("file build root has type %v, want a file key", typ)
	}
}

func TestObjects_RejectsMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, _, err := Objects(missing, Opts{}); err == nil {
		t.Errorf("expected error building a missing path")
	}
}

// TestDir_WritesToPackstore ingests into a real packstore and checks the
// stored root object round-trips, and that the build is deterministic across
// worker counts.
func TestDir_WritesToPackstore(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)

	var roots []key.Key
	for _, jobs := range []int{1, 8} {
		st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
		if err != nil {
			t.Fatal(err)
		}
		root, stats, err := Dir(st, src, Opts{Jobs: jobs})
		if err != nil {
			t.Fatalf("Dir with %d jobs: %v", jobs, err)
		}
		if stats.Stored == 0 {
			t.Fatalf("Dir with %d jobs stored no objects", jobs)
		}
		if _, err := st.Get(root); err != nil {
			t.Fatalf("root object not retrievable: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}
	if roots[0] != roots[1] {
		t.Fatalf("root differs across jobs: %s vs %s", roots[0], roots[1])
	}
}

// countingProgress counts progress events; safe for concurrent use.
type countingProgress struct {
	files atomic.Int64
	bytes atomic.Int64
}

func (p *countingProgress) FileDone()      { p.files.Add(1) }
func (p *countingProgress) AddBytes(n int) { p.bytes.Add(int64(n)) }

func TestObjects_ReportsProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}
	p := &countingProgress{}
	seq, _, err := Objects(dir, Opts{Jobs: 2, Progress: p})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := p.bytes.Load(); got != 11 {
		t.Errorf("bytes = %d, want 11", got)
	}
	if got := p.files.Load(); got != 2 {
		t.Errorf("files = %d, want 2", got)
	}
}

// writeDeepTree creates a deterministic, deep and wide directory tree: nested
// subdirectories each holding several small files plus one large multi-chunk
// file, so ingestion fans out across many files and subtrees.
func writeDeepTree(t *testing.T, root string) {
	t.Helper()
	var build func(dir string, depth, seed int)
	build = func(dir string, depth, seed int) {
		// A large file forces CDC into many chunks and a multi-level file index.
		large := make([]byte, 256<<10)
		fillPseudoRandom(large, uint64(seed)*1_000_003+7)
		if err := os.WriteFile(filepath.Join(dir, "large.bin"), large, 0o644); err != nil {
			t.Fatal(err)
		}
		// Several small files create multiple DirLeaf/DirNode objects.
		for i := range 6 {
			name := fmt.Sprintf("file-%02d.txt", i)
			content := fmt.Appendf(nil, "depth=%d seed=%d index=%d payload", depth, seed, i)
			if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if depth == 0 {
			return
		}
		for i := range 3 {
			sub := filepath.Join(dir, fmt.Sprintf("sub-%d", i))
			if err := os.Mkdir(sub, 0o755); err != nil {
				t.Fatal(err)
			}
			build(sub, depth-1, seed*10+i+1)
		}
	}
	build(root, 3, 1)
}

// fillPseudoRandom fills b with a deterministic byte stream (a splitmix64-style
// generator) so the large test files have enough entropy for content-defined
// chunking to find boundaries, while remaining reproducible.
func fillPseudoRandom(b []byte, seed uint64) {
	x := seed
	for i := 0; i+8 <= len(b); i += 8 {
		x += 0x9E3779B97F4A7C15
		z := x
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z = z ^ (z >> 31)
		binary.LittleEndian.PutUint64(b[i:], z)
	}
}
