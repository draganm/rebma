package refstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/draganm/rebma/refstore/internal/refsdb"
	"golang.org/x/sys/unix"
)

// Releases before the SQLite store kept references in a Pebble DB in the same
// directory. Open imports such a store once. This file is the only importer
// of Pebble and goes away with migration support.
const (
	// legacyLock exists in every Pebble directory, so one stat per Open
	// decides whether there is anything to do.
	legacyLock = "LOCK"
	// legacyManifest prefixes the marker naming Pebble's current manifest:
	// its presence means a Pebble store, not just debris.
	legacyManifest = "marker.manifest."
	// legacyDir keeps the Pebble files after the import, as a backup.
	legacyDir = "pebble-migrated"
	// migrateLock serializes concurrent opens of a store being migrated.
	migrateLock = "migrate.lock"
	// poisonFile makes Pebble refuse the directory ("unknown format major
	// version 999"). Without it a binary that predates this change would
	// create an empty Pebble store here, report no references, and a gc run
	// from it would reap every object.
	poisonFile = "marker.format-version.999999.999"
)

// discardLogger silences pebble's internal logging.
type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("refstore: pebble fatal: "+format, args...))
}

// migrateLegacy imports a Pebble reference store found in dir and retires
// its files. The rename of the finished database to refs.sqlite is the
// commit point: before it a crash leaves the Pebble store untouched, after
// it the import is never repeated, so later writes are never overwritten.
func migrateLegacy(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, legacyLock)); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	unlock, err := lockMigration(dir)
	if err != nil {
		return err
	}
	defer unlock()

	// Read the directory only now: another process may have finished the
	// migration while this one waited for the lock.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	var files []string
	legacy := false
	for _, e := range entries {
		if !e.IsDir() && isPebbleFile(e.Name()) {
			files = append(files, e.Name())
			legacy = legacy || strings.HasPrefix(e.Name(), legacyManifest)
		}
	}
	if !legacy {
		// Debris. After a migration: a Pebble-based binary tried the store,
		// created its lock file and failed on the poison marker, or a power
		// loss undid part of the move; all of it goes where the rest went.
		// Otherwise there never was a Pebble store here, only a lock file.
		if _, err := os.Stat(filepath.Join(dir, dbFile)); err == nil {
			return retireLegacy(dir, files)
		}
		if err := os.Remove(filepath.Join(dir, legacyLock)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("refstore: %w", err)
		}
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, dbFile)); errors.Is(err, fs.ErrNotExist) {
		if err := importPebble(dir); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	return retireLegacy(dir, files)
}

func isPebbleFile(name string) bool {
	switch {
	case name == poisonFile:
		return false
	case name == legacyLock, name == "CURRENT":
		return true
	}
	for _, prefix := range []string{"MANIFEST-", "OPTIONS-", "marker.", "temporary."} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	for _, suffix := range []string{".log", ".sst", ".dbtmp"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func lockMigration(dir string) (unlock func(), err error) {
	f, err := os.OpenFile(filepath.Join(dir, migrateLock), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("refstore: migration lock: %w", err)
	}
	for {
		if err = unix.Flock(int(f.Fd()), unix.LOCK_EX); err != unix.EINTR {
			break
		}
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("refstore: migration lock: %w", err)
	}
	return func() { f.Close() }, nil // closing the file releases the lock
}

// importPebble copies every record into a new database and renames it into
// place. The copy is built with a rollback journal and full syncs, so once
// closed it is a single durable file that a rename can publish. Pebble opens
// read-only, which still takes its directory lock: a store in use by an old
// binary fails the migration instead of being copied mid-flight.
func importPebble(dir string) (err error) {
	pdb, err := pebble.Open(dir, &pebble.Options{ReadOnly: true, Logger: discardLogger{}})
	if err != nil {
		return fmt.Errorf("refstore: opening the Pebble store in %s to migrate it: %w", dir, err)
	}
	defer pdb.Close()

	tmp := filepath.Join(dir, dbFile+".tmp")
	for _, stale := range []string{tmp, tmp + "-journal"} {
		if err := os.Remove(stale); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("refstore: %w", err)
		}
	}
	db, err := openDB(tmp, true, false)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("refstore: migrating: %w", err)
	}
	// Unconditional, and a no-op once committed: a panic must not leave the
	// write lock held, which would wedge every writer in every process.
	defer tx.Rollback()
	q := refsdb.New(tx)
	it, err := pdb.NewIter(&pebble.IterOptions{})
	if err != nil {
		return fmt.Errorf("refstore: migrating: %w", err)
	}
	for it.First(); it.Valid(); it.Next() {
		if err = q.PutRecord(ctx, refsdb.PutRecordParams{Name: blob(slices.Clone(it.Key())), Record: blob(slices.Clone(it.Value()))}); err != nil {
			it.Close()
			return fmt.Errorf("refstore: migrating: %w", err)
		}
	}
	if err = errors.Join(it.Error(), it.Close()); err != nil {
		return fmt.Errorf("refstore: reading the Pebble store: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("refstore: migrating: %w", err)
	}
	if err = db.Close(); err != nil {
		return fmt.Errorf("refstore: migrating: %w", err)
	}
	if err = os.Rename(tmp, filepath.Join(dir, dbFile)); err != nil {
		return fmt.Errorf("refstore: migrating: %w", err)
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	// Poison the directory while Pebble's lock is still held (pdb closes on
	// return): otherwise a Pebble-based binary could open the store in the
	// gap and write references that the migration has already left behind.
	// Not before the rename: a crash in between would leave a store this
	// import could not open again.
	return writePoison(dir)
}

func writePoison(dir string) error {
	if err := os.WriteFile(filepath.Join(dir, poisonFile), nil, 0o644); err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	return nil
}

// retireLegacy writes the poison marker and moves Pebble's files aside: data
// files first, the manifest markers next, LOCK last. A crash leaves LOCK —
// and, until the very end, a manifest marker — in place, so the next Open
// comes back here and finishes.
func retireLegacy(dir string, files []string) error {
	if err := writePoison(dir); err != nil { // again: a resumed cleanup may predate it
		return err
	}
	aside := filepath.Join(dir, legacyDir)
	if err := os.MkdirAll(aside, 0o755); err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	rank := func(name string) int {
		switch {
		case name == legacyLock:
			return 2
		case strings.HasPrefix(name, legacyManifest):
			return 1
		}
		return 0
	}
	slices.SortStableFunc(files, func(a, b string) int { return rank(a) - rank(b) })
	for _, name := range files {
		if err := os.Rename(filepath.Join(dir, name), filepath.Join(aside, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("refstore: retiring the Pebble store: %w", err)
		}
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("refstore: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("refstore: syncing %s: %w", dir, err)
	}
	return nil
}
