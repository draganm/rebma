package main

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
)

// runApp runs the CLI with args (without the leading program name) and
// returns everything it printed to the app writer.
func runApp(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	err := app.Run(append([]string{"amber-store"}, args...))
	return buf.String(), err
}

// writeFixture builds a small source tree: files, a subdirectory, a symlink.
func writeFixture(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_IngestLsExportRestore(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()

	// Ingest, recording a reference.
	out, err := runApp(t, "--store", store, "ingest", "--no-progress", "--ref", "snap", src)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out)
	if root == "" {
		t.Fatal("ingest printed no root key")
	}

	// ls by reference and by key, root and subpath.
	for _, spec := range []string{"ref:snap", root, "ref:snap@sub", root + "/sub"} {
		out, err := runApp(t, "--store", store, "ls", spec)
		if err != nil {
			t.Fatalf("ls %s: %v", spec, err)
		}
		want := "a.txt"
		if strings.Contains(spec, "sub") {
			want = "b.txt"
		}
		if !strings.Contains(out, want) {
			t.Errorf("ls %s output %q does not mention %s", spec, out, want)
		}
	}
	out, err = runApp(t, "--store", store, "ls", "ref:snap")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "link -> a.txt") {
		t.Errorf("ls output %q does not render the symlink", out)
	}

	// Export to a tar file.
	tarPath := filepath.Join(t.TempDir(), "snap.tar")
	if _, err := runApp(t, "--store", store, "export", "-o", tarPath, "ref:snap"); err != nil {
		t.Fatalf("export: %v", err)
	}
	if info, err := os.Stat(tarPath); err != nil || info.Size() == 0 {
		t.Fatalf("export wrote no tar: %v", err)
	}

	// Restore and compare with the source.
	dest := filepath.Join(t.TempDir(), "restored")
	if _, err := runApp(t, "--store", store, "restore", "ref:snap", dest); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for _, f := range []struct{ rel, content string }{
		{"a.txt", "alpha"},
		{filepath.Join("sub", "b.txt"), "beta"},
	} {
		got, err := os.ReadFile(filepath.Join(dest, f.rel))
		if err != nil {
			t.Fatalf("restored %s: %v", f.rel, err)
		}
		if string(got) != f.content {
			t.Errorf("restored %s = %q, want %q", f.rel, got, f.content)
		}
	}
	if target, err := os.Readlink(filepath.Join(dest, "link")); err != nil || target != "a.txt" {
		t.Errorf("restored symlink target = %q (%v), want a.txt", target, err)
	}
	if info, err := os.Stat(filepath.Join(dest, "sub", "b.txt")); err == nil {
		if info.Mode().Perm() != 0o600 {
			t.Errorf("restored sub/b.txt mode = %o, want 600", info.Mode().Perm())
		}
	}
}

func TestE2E_RefCommands(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()

	out, err := runApp(t, "--store", store, "ingest", "--no-progress", src)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out)

	if _, err := runApp(t, "--store", store, "ref", "set", "v1", root); err != nil {
		t.Fatalf("ref set: %v", err)
	}
	out, err = runApp(t, "--store", store, "ref", "get", "v1")
	if err != nil {
		t.Fatalf("ref get: %v", err)
	}
	if strings.TrimSpace(out) != root {
		t.Errorf("ref get = %q, want %q", strings.TrimSpace(out), root)
	}
	out, err = runApp(t, "--store", store, "ref", "list")
	if err != nil {
		t.Fatalf("ref list: %v", err)
	}
	if !strings.Contains(out, "v1") || !strings.Contains(out, root) {
		t.Errorf("ref list %q missing v1 / root", out)
	}
	if _, err := runApp(t, "--store", store, "ref", "rm", "v1"); err != nil {
		t.Fatalf("ref rm: %v", err)
	}
	if _, err := runApp(t, "--store", store, "ref", "get", "v1"); err == nil {
		t.Error("ref get after rm should fail")
	}
}

func TestE2E_NoIgnoreFlag(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, ".amberignore"), []byte("*.log\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "keep.txt"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "drop.log"), []byte("dropped"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()

	withIgnore, err := runApp(t, "--store", store, "ingest", "--no-progress", src)
	if err != nil {
		t.Fatal(err)
	}
	withoutIgnore, err := runApp(t, "--store", store, "ingest", "--no-progress", "--no-ignore", src)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(withIgnore) == strings.TrimSpace(withoutIgnore) {
		t.Error("--no-ignore must change the root key")
	}
}

func TestE2E_MissingStoreFlag(t *testing.T) {
	t.Setenv("AMBER_STORE", "")
	if _, err := runApp(t, "ls", strings.Repeat("00", 32)); err == nil {
		t.Error("expected an error without --store / $AMBER_STORE")
	}
}

func TestE2E_RefSetChecksCompleteness(t *testing.T) {
	store := t.TempDir()
	// A syntactically valid key that names nothing in the store.
	bogus := strings.Repeat("00", 32)
	if _, err := runApp(t, "--store", store, "ref", "set", "v1", bogus); err == nil {
		t.Error("ref set to an absent key succeeded")
	}
}

func TestE2E_RefLifecycle(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()
	seg := []string{"--store", store, "--segment-size", "4096"}
	out, err := runApp(t, append(seg, "ingest", "--no-progress", src)...)
	if err != nil {
		t.Fatal(err)
	}
	root := strings.TrimSpace(out)
	if _, err := runApp(t, append(seg, "ref", "set", "v1", root)...); err != nil {
		t.Fatalf("ref set: %v", err)
	}
	// A second name shares the root; removing one keeps the tree live.
	if _, err := runApp(t, append(seg, "ref", "set", "v2", root)...); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, append(seg, "ref", "rm", "v1")...); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := runApp(t, append(seg, "gc", "run", "--grace", "1ms", "--garbage", "0")...); err != nil {
		t.Fatalf("gc run while v2 lives: %v", err)
	}
	restoreDir := t.TempDir()
	if _, err := runApp(t, append(seg, "restore", "ref:v2", restoreDir)...); err != nil {
		t.Fatalf("restore after gc while v2 lives: %v", err)
	}
	// The last rm makes the tree garbage; the next cycle collects it.
	if _, err := runApp(t, append(seg, "ref", "rm", "v2")...); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := runApp(t, append(seg, "gc", "run", "--grace", "1ms", "--garbage", "0")...); err != nil {
		t.Fatalf("gc run after last rm: %v", err)
	}
	out, err = runApp(t, append(seg, "gc", "why", root)...)
	if err != nil {
		t.Fatalf("gc why: %v", err)
	}
	if !strings.Contains(out, "unreferenced") {
		t.Errorf("gc why after last rm = %q, want unreferenced", out)
	}
}

func TestE2E_GC(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()
	seg := []string{"--store", store, "--segment-size", "4096"}

	out, err := runApp(t, append(seg, "ingest", "--no-progress", "--ref", "v1", src)...)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root1 := strings.TrimSpace(out)

	// The tree changes; v1 moves on, orphaning the first tree's unique data.
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte(strings.Repeat("fresh content\n", 200)), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err = runApp(t, append(seg, "ingest", "--no-progress", "--ref", "v1", src)...)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	root2 := strings.TrimSpace(out)
	if root1 == root2 {
		t.Fatal("fixture change did not change the root")
	}

	// status runs and mentions the store's packs.
	out, err = runApp(t, append(seg, "gc", "status")...)
	if err != nil {
		t.Fatalf("gc status: %v", err)
	}
	if !strings.Contains(out, "live") {
		t.Errorf("gc status output %q missing totals", out)
	}

	// A forced run with a tiny grace reaps the dead majority. (References
	// written by ingest --ref carry closures since the collector wiring
	// landed; the cycle would also walk any that were missing.)
	time.Sleep(50 * time.Millisecond) // put seals safely behind a 1ms grace
	out, err = runApp(t, append(seg, "gc", "run", "--grace", "1ms", "--garbage", "0")...)
	if err != nil {
		t.Fatalf("gc run: %v", err)
	}
	if !strings.Contains(out, "reaped") {
		t.Errorf("gc run output %q missing summary", out)
	}

	// why: the new root is held by v1; the old root by nobody.
	out, err = runApp(t, append(seg, "gc", "why", root2)...)
	if err != nil {
		t.Fatalf("gc why: %v", err)
	}
	if !strings.Contains(out, "v1") {
		t.Errorf("gc why %q missing v1", out)
	}
	out, err = runApp(t, append(seg, "gc", "why", root1)...)
	if err != nil {
		t.Fatalf("gc why old: %v", err)
	}
	if strings.Contains(out, "v1") {
		t.Errorf("gc why on dead root still names v1: %q", out)
	}

	// The referenced tree is fully intact after the sweep.
	tarPath := filepath.Join(t.TempDir(), "out.tar")
	if _, err := runApp(t, append(seg, "export", "-o", tarPath, "ref:v1")...); err != nil {
		t.Fatalf("export after gc: %v", err)
	}
	restoreDir := t.TempDir()
	if _, err := runApp(t, append(seg, "restore", "ref:v1", restoreDir)...); err != nil {
		t.Fatalf("restore after gc: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restoreDir, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(got), "fresh content") {
		t.Error("restored content wrong after gc")
	}
}

func TestE2E_Commit(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()
	seg := []string{"--store", store, "--segment-size", "4096"}
	run := func(args ...string) string {
		t.Helper()
		out, err := runApp(t, append(slices.Clone(seg), args...)...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return strings.TrimSpace(out)
	}
	create := []string{"commit", "create", "--ref", "main",
		"--author", "Ann <ann@example.com>", "--date", "2026-01-02T03:04:05+01:00"}

	root1 := run("ingest", "--no-progress", src)
	c1 := run(append(slices.Clone(create), "-m", "first", root1)...)

	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha, revised"), 0o644); err != nil {
		t.Fatal(err)
	}
	root2 := run("ingest", "--no-progress", src)
	c2 := run(append(slices.Clone(create), "--parent", "ref:main", "--change-id", "00ff10", "-m", "second", root2)...)
	for _, bad := range []string{"xyz", ""} {
		if _, err := runApp(t, append(slices.Clone(seg), append(slices.Clone(create), "--change-id", bad, "-m", "bad", root2)...)...); err == nil {
			t.Errorf("commit create accepted the change id %q", bad)
		}
	}
	if c1 == c2 || c2 == root2 {
		t.Fatalf("commit keys: c1 %s, c2 %s, root2 %s", c1, c2, root2)
	}
	if got := run("ref", "get", "main"); got != c2 {
		t.Errorf("ref get main = %s, want the second commit %s", got, c2)
	}

	show := run("commit", "show", "ref:main")
	for _, want := range []string{
		"commit " + c2, "tree " + root2, "parent " + c1, "change-id 00ff10",
		"author Ann <ann@example.com> 2026-01-02T03:04:05+01:00", "    second",
	} {
		if !strings.Contains(show, want) {
			t.Errorf("commit show output %q is missing %q", show, want)
		}
	}

	// A commit stands in for its tree wherever a directory is expected.
	for spec, want := range map[string]string{"ref:main": "a.txt", "ref:main@sub": "b.txt", c1: "a.txt", c1 + "/sub": "b.txt"} {
		if out := run("ls", spec); !strings.Contains(out, want) {
			t.Errorf("ls %s output %q does not mention %s", spec, out, want)
		}
	}

	// History is reachable from the branch, so a forced gc keeps the first
	// commit's tree: it still restores, with the original content.
	time.Sleep(50 * time.Millisecond)
	run("gc", "run", "--grace", "1ms", "--garbage", "0")
	for commitKey, want := range map[string]string{c1: "alpha", c2: "alpha, revised"} {
		dest := filepath.Join(t.TempDir(), "restored")
		run("restore", commitKey, dest)
		got, err := os.ReadFile(filepath.Join(dest, "a.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("restored a.txt of %s = %q, want %q", commitKey, got, want)
		}
	}

	// Rejections: a tree as a parent, a file as the tree, a missing author.
	for name, args := range map[string][]string{
		"tree as parent": {"commit", "create", "--author", "Ann", "-m", "x", "--parent", root1, root2},
		"file as tree":   {"commit", "create", "--author", "Ann", "-m", "x", root2 + "/a.txt"},
		"no author":      {"commit", "create", "-m", "x", root2},
		"show a tree":    {"commit", "show", root2},
	} {
		if _, err := runApp(t, append(slices.Clone(seg), args...)...); err == nil {
			t.Errorf("%s: command succeeded", name)
		}
	}
}

// A directory entry may hold a commit. Every file operation reads through it
// to the commit's tree: the commit object itself is skipped.
func TestE2E_CommitInsideADirectory(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src) // a.txt, sub/b.txt, link
	store := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		out, err := runApp(t, append([]string{"--store", store}, args...)...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return strings.TrimSpace(out)
	}
	root := run("ingest", "--no-progress", src)
	vendored := run("commit", "create", "--author", "Ann <ann@example.com>", "-m", "vendored", root)

	// No command builds such a tree (ingest reads a filesystem, which has no
	// commits), so the test does, through the library.
	raw, err := hex.DecodeString(vendored)
	if err != nil {
		t.Fatal(err)
	}
	ck, err := key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := packstore.Open(filepath.Join(store, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	mainBlob, err := fstree.EncodeBlob([]byte("package main"))
	if err != nil {
		t.Fatal(err)
	}
	holder, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("main.go"), Mode: 0o100644, ContentKey: mainBlob.Key[:]},
		{Name: []byte("vendor"), Mode: 0o040755, ContentKey: ck[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{mainBlob, holder} {
		if err := objects.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	top := holder.Key.String()

	for spec, want := range map[string]string{top: "vendor", top + "/vendor": "a.txt", top + "/vendor/sub": "b.txt"} {
		if out := run("ls", spec); !strings.Contains(out, want) {
			t.Errorf("ls %s output %q does not mention %s", spec, out, want)
		}
	}

	tarPath := filepath.Join(t.TempDir(), "top.tar")
	run("export", "-o", tarPath, top)
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	archived := map[string]string{}
	for tr := tar.NewReader(f); ; {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		archived[h.Name] = string(data)
	}
	if archived["vendor/a.txt"] != "alpha" || archived["vendor/sub/b.txt"] != "beta" {
		t.Errorf("the archive does not hold the commit's tree under vendor/: %v", archived)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	run("restore", top, dest)
	for name, want := range map[string]string{"main.go": "package main", "vendor/a.txt": "alpha", "vendor/sub/b.txt": "beta"} {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil || string(got) != want {
			t.Errorf("restored %s = %q, %v; want %q", name, got, err, want)
		}
	}

	// TREE may name a commit through such an entry: the new commit records
	// that commit's tree, not the commit.
	regraft := run("commit", "create", "--author", "Ann <ann@example.com>", "-m", "regraft", top+"/vendor")
	if show := run("commit", "show", regraft); !strings.Contains(show, "tree "+root) {
		t.Errorf("commit show %q does not record the vendored commit's tree %s", show, root)
	}
}

// A commit keyed by the first release's rule, its own bytes alone, is not a
// commit any more. Nothing may be built on it: not a child commit, not a
// reference, not a listing. commit show still prints it, which is how its
// tree is found again.
func TestE2E_CommitKeyedByTheOldRuleIsRefused(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()
	run := func(args ...string) (string, error) {
		t.Helper()
		out, err := runApp(t, append([]string{"--store", store}, args...)...)
		return strings.TrimSpace(out), err
	}
	root, err := run("ingest", "--no-progress", src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(root)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	id := commit.Identity{Name: "Ann", When: 1}
	_, data, err := commit.Commit{Tree: tree, Author: id, Committer: id, Message: "from the first release"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	old, err := key.New(key.Commit, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := packstore.Open(filepath.Join(store, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(old, data); err != nil { // Put trusts its caller, as it did then
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	if out, err := run("commit", "show", old.String()); err != nil || !strings.Contains(out, "tree "+root) {
		t.Errorf("commit show = %q, %v: it should still print the commit, and so its tree", out, err)
	}
	for _, args := range [][]string{
		{"commit", "create", "--author", "Ann <ann@example.com>", "--parent", old.String(), "-m", "child", root},
		{"ref", "set", "old", old.String()},
		{"ls", old.String()},
	} {
		if out, err := run(args...); err == nil || !strings.Contains(err.Error(), "footprint") {
			t.Errorf("%v = %q, %v; want an error that names the footprint rule", args, out, err)
		}
	}
}

// Only a directory entry may hold a commit. Under an entry of another type it
// is a malformed tree, which path resolution refuses.
func TestE2E_CommitUnderARegularFileEntryIsRefused(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()
	run := func(args ...string) (string, error) {
		t.Helper()
		out, err := runApp(t, append([]string{"--store", store}, args...)...)
		return strings.TrimSpace(out), err
	}
	root, err := run("ingest", "--no-progress", src)
	if err != nil {
		t.Fatal(err)
	}
	vendored, err := run("commit", "create", "--author", "Ann <ann@example.com>", "-m", "vendored", root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(vendored)
	if err != nil {
		t.Fatal(err)
	}
	ck, err := key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	holder, err := fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("odd"), Mode: 0o100644, ContentKey: ck[:]}})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := packstore.Open(filepath.Join(store, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(holder.Key, holder.Bytes); err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run("ls", holder.Key.String()+"/odd/sub"); err == nil {
		t.Errorf("ls through a regular-file entry that holds a commit = %q, want an error", out)
	}
}

func TestE2E_RefExpect(t *testing.T) {
	store := t.TempDir()
	ingest := func(extra string) string {
		src := t.TempDir()
		writeFixture(t, src)
		if err := os.WriteFile(filepath.Join(src, "extra.txt"), []byte(extra), 0o644); err != nil {
			t.Fatal(err)
		}
		out, err := runApp(t, "--store", store, "ingest", "--no-progress", src)
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
		return strings.TrimSpace(out)
	}
	root1, root2 := ingest("one"), ingest("two")
	ref := func(args ...string) (string, error) {
		out, err := runApp(t, append([]string{"--store", store, "ref"}, args...)...)
		return strings.TrimSpace(out), err
	}
	wantAt := func(want string) {
		t.Helper()
		if got, err := ref("get", "r"); err != nil || got != want {
			t.Fatalf("ref get r = %q, %v; want %s", got, err, want)
		}
	}

	if _, err := ref("set", "--expect", "none", "r", root1); err != nil {
		t.Fatalf("create with --expect none: %v", err)
	}
	if _, err := ref("set", "--expect", "none", "r", root2); err == nil {
		t.Fatal("--expect none overwrote an existing reference")
	}
	wantAt(root1)
	if _, err := ref("set", "--expect", root2, "r", root2); err == nil {
		t.Fatal("a stale --expect moved the reference")
	}
	wantAt(root1)
	// A flag after the positionals is not parsed as a flag; it must fail
	// rather than turn into an unconditional set.
	if _, err := ref("set", "r", root2, "--expect", root2); err == nil {
		t.Fatal("a misplaced --expect was accepted")
	}
	wantAt(root1)
	// An empty expectation, a script's unset variable, must fail as well.
	if _, err := ref("set", "--expect", "", "r", root2); err == nil {
		t.Fatal("an empty --expect was accepted by ref set")
	}
	wantAt(root1)
	if _, err := ref("set", "--expect", root1, "r", root2); err != nil {
		t.Fatalf("set with the right --expect: %v", err)
	}
	wantAt(root2)

	if _, err := ref("rm", "--expect", "none", "r"); err == nil {
		t.Fatal("ref rm accepted --expect none")
	}
	if _, err := ref("rm", "--expect", "", "r"); err == nil {
		t.Fatal("an empty --expect was accepted by ref rm")
	}
	wantAt(root2)
	if _, err := ref("rm", "--expect", root1, "r"); err == nil {
		t.Fatal("a stale --expect deleted the reference")
	}
	wantAt(root2)
	if _, err := ref("rm", "--expect", root2, "r"); err != nil {
		t.Fatalf("rm with the right --expect: %v", err)
	}
	if _, err := ref("get", "r"); err == nil {
		t.Fatal("the reference survived ref rm")
	}
	if _, err := ref("set", "--expect", root1, "gone", root1); err == nil {
		t.Fatal("--expect KEY created a reference that did not exist")
	}
}
