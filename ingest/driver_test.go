package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
)

func TestBuildDir_FailFastOnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "secret")
	if err := os.WriteFile(p, []byte("data"), 0o000); err != nil {
		t.Fatal(err)
	}
	d := &driver{ic: chunkers.NewItemChunker(7), xattrInlineMax: 256}
	emit := func(fstree.Object) error { return nil }
	if _, err := d.buildDir(dir, nil, emit); err == nil {
		t.Errorf("expected buildDir to fail on an unreadable file")
	}
}
