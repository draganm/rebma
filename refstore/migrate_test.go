package refstore_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/draganm/rebma/refstore"
)

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(string, ...any) {}

// legacyStore writes a Pebble reference store the way the previous release
// did. flushed records end up in an sstable, the rest only in the
// write-ahead log.
func legacyStore(t *testing.T, dir string, flushed, logged map[string]string) {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range flushed {
		if err := db.Set([]byte(k), []byte(v), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for k, v := range logged {
		if err := db.Set([]byte(k), []byte(v), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

func wantRecords(t *testing.T, s *refstore.Store, want map[string]string) {
	t.Helper()
	all, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range all {
		got[r.Name] = string(r.Data)
	}
	if len(got) != len(want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("records = %v, want %v", got, want)
		}
	}
}

func TestMigratesLegacyPebbleStore(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a/flushed": "1", "": "empty-name"}, map[string]string{"b/logged": "2"})
	want := map[string]string{"a/flushed": "1", "": "empty-name", "b/logged": "2"}

	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	wantRecords(t, s, want)
	if err := s.Put("c/new", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	want["c/new"] = "3"

	if exists(t, filepath.Join(dir, "LOCK")) {
		t.Error("Pebble's LOCK file is still in the store directory")
	}
	if !exists(t, filepath.Join(dir, "pebble-migrated", "LOCK")) {
		t.Error("the Pebble files were not kept in pebble-migrated/")
	}
	if !exists(t, filepath.Join(dir, "marker.format-version.999999.999")) {
		t.Error("the poison marker is missing")
	}
	if exists(t, filepath.Join(dir, "refs.sqlite.tmp")) {
		t.Error("the import's temporary database was left behind")
	}
	wantRecords(t, open(t, dir), want) // a second open imports nothing again
}

func TestMigratedStoreRefusesOldPebbleBinaries(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	open(t, dir).Close()
	db, err := pebble.Open(dir, &pebble.Options{Logger: quietLogger{}})
	if err == nil {
		db.Close()
		t.Fatal("a Pebble-based binary can still open the migrated directory; it would see no references")
	}
}

func TestEmptyLegacyStoreMigrates(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, nil, nil)
	wantRecords(t, open(t, dir), map[string]string{})
	if exists(t, filepath.Join(dir, "LOCK")) {
		t.Error("Pebble's LOCK file is still in the store directory")
	}
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(to, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(from, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A crash after the import committed but before the Pebble files were moved
// leaves both side by side. The next open must finish the cleanup without
// importing again, or it would overwrite newer data with the old.
func TestInterruptedCleanupDoesNotReimport(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "old"}, nil)
	backup := t.TempDir()
	copyDir(t, dir, backup)

	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	copyDir(t, backup, dir) // the Pebble files are back, next to refs.sqlite

	wantRecords(t, open(t, dir), map[string]string{"a": "new"})
	if exists(t, filepath.Join(dir, "LOCK")) {
		t.Error("the resumed cleanup left Pebble's LOCK file behind")
	}
}

// An old binary that tries a migrated store creates LOCK before it fails on
// the poison marker.
func TestStrayPebbleLockIsRemoved(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	open(t, dir).Close()
	if err := os.WriteFile(filepath.Join(dir, "LOCK"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wantRecords(t, open(t, dir), map[string]string{"a": "1"})
	if exists(t, filepath.Join(dir, "LOCK")) {
		t.Error("the stray LOCK file was not removed")
	}
}

func TestFilesThatAreNotPebblesStayPut(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	if err := os.WriteFile(filepath.Join(dir, "NOTES.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	open(t, dir).Close()
	if !exists(t, filepath.Join(dir, "NOTES.txt")) {
		t.Error("a file that is not Pebble's was moved away")
	}
}

func TestConcurrentOpensMigrateOnce(t *testing.T) {
	dir := t.TempDir()
	want := map[string]string{"a": "1", "b": "2"}
	legacyStore(t, dir, want, nil)
	const n = 4
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
			t.Fatalf("open %d: %v", i, err)
		}
		wantRecords(t, stores[i], want)
	}
}

// A Pebble store that an old binary still has open cannot be migrated: it
// must be left exactly as it is.
func TestMigrationRefusesAStoreInUse(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	inUse, err := pebble.Open(dir, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := refstore.Open(dir, false); err == nil {
		inUse.Close()
		t.Fatal("Open migrated a Pebble store that another owner holds open")
	}
	for _, name := range []string{"refs.sqlite", "marker.format-version.999999.999", "pebble-migrated"} {
		if exists(t, filepath.Join(dir, name)) {
			t.Errorf("the refused migration left %s behind", name)
		}
	}
	if err := inUse.Set([]byte("b"), []byte("2"), pebble.Sync); err != nil {
		t.Fatalf("the old owner can no longer write: %v", err)
	}
	if err := inUse.Close(); err != nil {
		t.Fatal(err)
	}
	wantRecords(t, open(t, dir), map[string]string{"a": "1", "b": "2"})
}

func TestStaleTemporaryDatabaseIsReplaced(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	for _, name := range []string{"refs.sqlite.tmp", "refs.sqlite.tmp-journal"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("left by a crashed import"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantRecords(t, open(t, dir), map[string]string{"a": "1"})
	if exists(t, filepath.Join(dir, "refs.sqlite.tmp")) {
		t.Error("the stale temporary database is still there")
	}
}

// A power loss can undo part of the move, leaving Pebble files without the
// manifest marker next to the finished database.
func TestLeftoverPebbleFilesAreRetired(t *testing.T) {
	dir := t.TempDir()
	legacyStore(t, dir, map[string]string{"a": "1"}, nil)
	open(t, dir).Close()
	for _, name := range []string{"LOCK", "000123.sst", "OPTIONS-000042"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantRecords(t, open(t, dir), map[string]string{"a": "1"})
	for _, name := range []string{"LOCK", "000123.sst", "OPTIONS-000042"} {
		if exists(t, filepath.Join(dir, name)) {
			t.Errorf("%s was left in the store directory", name)
		}
	}
}
