package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/packstore"
)

// excludeFixture is a tree with a metadata dir at the root and a same-named
// dir one level down, which must not be excluded.
func excludeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644))
	must(os.MkdirAll(filepath.Join(dir, ".meta"), 0o755))
	must(os.WriteFile(filepath.Join(dir, ".meta", "junk"), make([]byte, 1000), 0o644))
	must(os.MkdirAll(filepath.Join(dir, "sub", ".meta"), 0o755))
	must(os.WriteFile(filepath.Join(dir, "sub", ".meta", "keep"), []byte("keep"), 0o644))
	return dir
}

func TestExclude_SkipsRootNameOnly(t *testing.T) {
	dir := excludeFixture(t)
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _, err := Dir(st, dir, Opts{Exclude: []string{".meta"}})
	if err != nil {
		t.Fatal(err)
	}
	// The same tree without the root .meta must give the same key.
	if err := os.RemoveAll(filepath.Join(dir, ".meta")); err != nil {
		t.Fatal(err)
	}
	want, _, err := Dir(st, dir, Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("root with Exclude %s != root without .meta %s", got, want)
	}
}

func TestExclude_IgnoresNoIgnore(t *testing.T) {
	dir := excludeFixture(t)
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _, err := Dir(st, dir, Opts{Exclude: []string{".meta"}, NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, ".meta")); err != nil {
		t.Fatal(err)
	}
	want, _, err := Dir(st, dir, Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Exclude must apply with NoIgnore: %s != %s", got, want)
	}
}

func TestScanWith_HonorsExclude(t *testing.T) {
	dir := excludeFixture(t)
	files, bytes, err := ScanWith(dir, Opts{Exclude: []string{".meta"}})
	if err != nil {
		t.Fatal(err)
	}
	// a.txt (5 bytes) and sub/.meta/keep (4 bytes); the root .meta/junk is skipped.
	if files != 2 || bytes != 9 {
		t.Fatalf("files=%d bytes=%d, want 2 and 9", files, bytes)
	}
}
