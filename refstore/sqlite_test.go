package refstore_test

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/draganm/rebma/refstore"
)

const (
	childDirEnv  = "REFSTORE_TEST_CHILD_DIR"
	childNameEnv = "REFSTORE_TEST_CHILD_NAME" // set: the child only opens and puts this name
)

// TestMain doubles as the second process of
// TestSecondProcessSharesTheStore: with childDirEnv set, the test binary
// opens that store, checks the parent's record, writes its own and exits.
func TestMain(m *testing.M) {
	if dir := os.Getenv(childDirEnv); dir != "" {
		if err := childProcess(dir); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func childProcess(dir string) error {
	s, err := refstore.Open(dir, false)
	if err != nil {
		return err
	}
	if name := os.Getenv(childNameEnv); name != "" {
		if err := s.Put(name, []byte("v")); err != nil {
			return err
		}
		return s.Close()
	}
	got, err := s.Get("from-parent")
	if err != nil {
		return fmt.Errorf("reading the parent's record: %w", err)
	}
	if string(got) != "p" {
		return fmt.Errorf("from-parent = %q, want p", got)
	}
	if err := s.Put("from-child", []byte("c")); err != nil {
		return err
	}
	return s.Close()
}

func TestSecondProcessSharesTheStore(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir) // stays open for the child's whole life
	if err := s.Put("from-parent", []byte("p")); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process: %v\n%s", err, out)
	}
	got, err := s.Get("from-child")
	if err != nil || string(got) != "c" {
		t.Fatalf("from-child = %q, %v; want c", got, err)
	}
}

func TestPutNilRecordReadsBackEmpty(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.Put("nil", nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("nil")
	if err != nil || len(got) != 0 {
		t.Fatalf("Get = %q, %v; want empty", got, err)
	}
}

func TestTwoHandlesShareOneStore(t *testing.T) {
	dir := t.TempDir()
	a, b := open(t, dir), open(t, dir)
	if err := a.Put("x", []byte("1")); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get("x")
	if err != nil || string(got) != "1" {
		t.Fatalf("second handle Get = %q, %v", got, err)
	}
	if err := b.Delete("x"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Get("x"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("first handle after the second's delete: %v, want ErrNotFound", err)
	}
}

func TestTwoHandlesKeepBatchesAtomic(t *testing.T) {
	dir := t.TempDir()
	handles := []*refstore.Store{open(t, dir), open(t, dir)}
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			s := handles[worker%2]
			for generation := range 50 {
				value := []byte(fmt.Sprintf("%d/%d", worker, generation))
				if err := s.PutBatch([]refstore.Record{{Name: "a", Data: value}, {Name: "b", Data: value}}); err != nil {
					t.Error(err)
					return
				}
				all, err := handles[(worker+1)%2].All()
				if err != nil {
					t.Error(err)
					return
				}
				if len(all) != 2 || !bytes.Equal(all[0].Data, all[1].Data) {
					t.Error("partial batch became visible")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestArbitraryNamesAndLargeRecords(t *testing.T) {
	s := open(t, t.TempDir())
	big := bytes.Repeat([]byte{0xab}, 100<<10)
	names := []string{"a", "a\x00b", "\xff\xfe", "b", ""}
	for _, n := range names {
		if err := s.Put(n, big); err != nil {
			t.Fatalf("Put(%q): %v", n, err)
		}
	}
	all, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"", "a", "a\x00b", "b", "\xff\xfe"}
	if len(all) != len(want) {
		t.Fatalf("All returned %d records, want %d", len(all), len(want))
	}
	for i, n := range want {
		if all[i].Name != n {
			t.Errorf("all[%d].Name = %q, want %q", i, all[i].Name, n)
		}
		if !bytes.Equal(all[i].Data, big) {
			t.Errorf("all[%d]: the record did not round-trip", i)
		}
	}
}

func TestOpenPathWithSpecialCharacters(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "we ird?#%41&=dir")
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "refs.sqlite")); err != nil {
		t.Fatalf("the database is not inside the store directory: %v", err)
	}
	got, err := open(t, dir).Get("k")
	if err != nil || string(got) != "v" {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}

func TestOpenRejectsForeignDatabase(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE other (x)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := refstore.Open(dir, false); err == nil || !strings.Contains(err.Error(), "not a reference store") {
		t.Fatalf("Open(foreign database) = %v, want a not-a-reference-store error", err)
	}
}

func TestOpenRejectsNonDatabaseFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "refs.sqlite"), bytes.Repeat([]byte("garbage "), 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := refstore.Open(dir, false); err == nil {
		t.Fatal("Open(garbage file) succeeded")
	}
}

// Switching a database into WAL mode takes an exclusive lock for which SQLite
// does not run the busy handler, so of several first opens of one store all
// but one used to fail with "database is locked".
func TestConcurrentFirstOpens(t *testing.T) {
	for round := range 10 {
		dir := t.TempDir()
		const n = 8
		stores := make([]*refstore.Store, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { stores[i], errs[i] = refstore.Open(dir, false) })
		}
		wg.Wait()
		for _, s := range stores {
			if s != nil {
				t.Cleanup(func() { s.Close() })
			}
		}
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d, open %d: %v", round, i, err)
			}
			if err := stores[i].Put(fmt.Sprintf("from-%d", i), []byte("v")); err != nil {
				t.Fatalf("round %d, put %d: %v", round, i, err)
			}
		}
		all, err := stores[0].All()
		if err != nil || len(all) != n {
			t.Fatalf("round %d: %d records, %v; want %d", round, len(all), err, n)
		}
		for _, s := range stores {
			s.Close()
		}
	}
}

// The same race between real processes: each child opens the fresh store
// and puts one record.
func TestConcurrentFirstOpensAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	const n = 6
	outs := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			cmd := exec.Command(os.Args[0])
			cmd.Env = append(os.Environ(), childDirEnv+"="+dir, fmt.Sprintf("%s=from-%d", childNameEnv, i))
			outs[i], errs[i] = cmd.CombinedOutput()
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("child %d: %v\n%s", i, err, outs[i])
		}
	}
	all, err := open(t, dir).All()
	if err != nil || len(all) != n {
		t.Fatalf("%d records, %v; want %d", len(all), err, n)
	}
}
