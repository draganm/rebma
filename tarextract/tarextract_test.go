package tarextract_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/rebma/tarextract"
)

func TestExtract_FilesDirsAndDeferredDirMeta(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 222_000_000)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(h *tar.Header, body string) {
		h.Format = tar.FormatPAX
		h.ModTime = mtime
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A read-only directory listed BEFORE its child file. If dir mode were applied
	// immediately, writing the child would fail; deferral makes it work.
	write(&tar.Header{Name: "ro/", Typeflag: tar.TypeDir, Mode: 0o500}, "")
	write(&tar.Header{Name: "ro/child.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5}, "hello")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "ro", "child.txt"))
	if err != nil {
		t.Fatalf("reading child: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("child content = %q, want hello", got)
	}
	di, err := os.Lstat(filepath.Join(dest, "ro"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o500 {
		t.Errorf("dir perm = %v, want 0500 (applied after children)", di.Mode().Perm())
	}
	if di.ModTime().UnixNano() != mtime.UnixNano() {
		t.Errorf("dir mtime = %d, want %d", di.ModTime().UnixNano(), mtime.UnixNano())
	}
	// Restore write permission so t.TempDir() cleanup can remove the directory.
	os.Chmod(filepath.Join(dest, "ro"), 0o700)
}

func TestExtract_RejectsUnsafeName(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	h := &tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1, Format: tar.FormatPAX}
	if err := tw.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err == nil {
		t.Fatalf("expected error extracting an escaping path")
	}
}

func TestExtract_SkipsPAXGlobalHeader(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	g := &tar.Header{
		Name:       "pax_global_header",
		Typeflag:   tar.TypeXGlobalHeader,
		PAXRecords: map[string]string{"comment": "0e7b0ccc10ca"},
		Format:     tar.FormatPAX,
	}
	if err := tw.WriteHeader(g); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "source/", Typeflag: tar.TypeDir, Mode: 0o755, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "source/README", Typeflag: tar.TypeReg, Mode: 0o644, Size: 2, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "source", "README"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Fatalf("README = %q, want hi", got)
	}
	if _, err := os.Lstat(filepath.Join(dest, "pax_global_header")); !os.IsNotExist(err) {
		t.Fatalf("pax_global_header was extracted: %v", err)
	}
}

func buildTar(t *testing.T, entries ...*tar.Header) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range entries {
		h.Format = tar.FormatPAX
		h.ModTime = time.Unix(1_700_000_000, 0)
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write(bytes.Repeat([]byte("x"), int(h.Size))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

// A symlink member followed by entries beneath it must not let the archive
// write through the link to a location outside destDir.
func TestExtract_RejectsWriteThroughSymlink(t *testing.T) {
	outside := t.TempDir()
	for name, entries := range map[string][]*tar.Header{
		"file through link": {
			{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside},
			{Name: "a/pwned", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
		},
		"dir through link": {
			{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside},
			{Name: "a/sub/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "a/sub/pwned", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
		},
		"dir entry on top of link": {
			{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside},
			{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
		},
	} {
		t.Run(name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "out")
			if err := tarextract.Extract(buildTar(t, entries...), dest); err == nil {
				t.Fatalf("expected error")
			}
			leaked, _ := os.ReadDir(outside)
			if len(leaked) != 0 {
				t.Fatalf("wrote outside destDir: %v", leaked)
			}
		})
	}
}

// The regular case still works: a symlink member sitting next to, not
// above, the entries that follow it.
func TestExtract_SymlinkSibling(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "out")
	err := tarextract.Extract(buildTar(t,
		&tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o755},
		&tar.Header{Name: "d/link", Typeflag: tar.TypeSymlink, Linkname: "/etc"},
		&tar.Header{Name: "d/f", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
	), dest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dest, "d", "f")); err != nil {
		t.Fatal(err)
	}
}

// chown(2) strips setuid/setgid, so ownership must be restored before the
// mode. Only observable when Extract actually chowns, i.e. as root.
func TestExtract_SetuidSurvivesChown(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to exercise the chown path")
	}
	dest := filepath.Join(t.TempDir(), "out")
	err := tarextract.Extract(buildTar(t,
		&tar.Header{Name: "suid", Typeflag: tar.TypeReg, Mode: 0o4755, Size: 1, Uid: 1, Gid: 1},
	), dest)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(filepath.Join(dest, "suid"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSetuid == 0 {
		t.Fatalf("setuid bit lost: mode %v", fi.Mode())
	}
}
