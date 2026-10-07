package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSmoke runs every phase end to end at a size that still seals and
// reaps packs: 30 refs at a tenth of the sizes over 4 MiB segments. The gc
// and verify phases exec the real CLI, built here from the module.
func TestSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the CLI and writes ~150 MiB")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
		if _, err := os.Stat(goBin); err != nil {
			t.Skip("no go binary to build the CLI with")
		}
	}
	dir := t.TempDir()
	cli := filepath.Join(dir, "amber-store")
	build := exec.Command(goBin, "build", "-o", cli, "github.com/draganm/rebma/cmd/amber-store")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the CLI: %v\n%s", err, out)
	}
	cfg := &config{
		data:    filepath.Join(dir, "data"),
		store:   filepath.Join(dir, "store"),
		bin:     cli,
		out:     filepath.Join(dir, "results.json"),
		restore: filepath.Join(dir, "restore"),
		refs:    30,
		scale:   0.1,
		segment: 4 << 20,
	}
	if err := run(cfg, "all"); err != nil {
		t.Fatal(err)
	}
	res, err := loadResults(cfg.out)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ingest) != 30 || res.DeleteN != 21 {
		t.Fatalf("ingested %d refs, deleted %d; want 30 and 21", len(res.Ingest), res.DeleteN)
	}
	var deduped int
	for _, x := range res.Ingest {
		deduped += x.Deduped
	}
	if deduped == 0 {
		t.Error("no object deduplicated: the shared clones did not overlap")
	}
	if len(res.GCRuns) != 2 {
		t.Fatalf("%d gc runs, want 2", len(res.GCRuns))
	}
	if !strings.Contains(res.GCRuns[0].Output, "reaped") || strings.Contains(res.GCRuns[0].Output, " 0 reaped") {
		t.Errorf("policy gc run reaped nothing: %q", res.GCRuns[0].Output)
	}
	if res.VerifyComplete != 9 || len(res.VerifyErrors) != 0 || len(res.VerifyRestoreOK) != 2 {
		t.Errorf("verify: %d complete, restores %v, errors %v", res.VerifyComplete, res.VerifyRestoreOK, res.VerifyErrors)
	}
	var buf bytes.Buffer
	if err := report(&buf, res); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"DATASET", "INGEST", "GC RUN", "RECLAIM", "VERIFY"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("report lacks %s:\n%s", want, buf.String())
		}
	}
}
