package ingest

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/draganm/rebma/amberignore"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// pbuilder builds the CAS tree concurrently. It reuses the driver's per-entry
// and per-file logic but fans the directory walk out across a bounded pool: each
// directory entry's subtree (a file's chunks or a subdirectory) is built
// independently, then the directory's own leaf/index objects are assembled in
// the original sorted-entry order. emit is the (concurrency-safe) sink shared by
// all workers.
type pbuilder struct {
	d    *driver
	emit fstree.Emit
	// sem bounds the number of in-flight worker goroutines. Offloading uses a
	// non-blocking send: when the pool is full, the work runs inline on the
	// current goroutine, so a parent never blocks waiting for a slot held by
	// one of its own descendants — the recursion cannot deadlock.
	sem chan struct{}
	// root and exclude implement Opts.Exclude: names in exclude are skipped
	// when path == root.
	root    string
	exclude map[string]bool
}

// buildDir builds the directory at path and returns its root key. Entries
// excluded by ign are skipped (excluded directories are pruned without being
// read). Sibling entries are built concurrently; the directory's leaf/index
// objects are then emitted in sorted-entry order, identical to the sequential
// walk.
func (b *pbuilder) buildDir(path string, ign *amberignore.Matcher, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	kept := make([]os.DirEntry, 0, len(ents))
	for _, de := range ents {
		if path == b.root && b.exclude[de.Name()] {
			continue
		}
		if !ign.Ignored(de.Name(), de.IsDir()) {
			kept = append(kept, de)
		}
	}

	entries := make([]fstree.Entry, len(kept))
	errs := make([]error, len(kept))
	var wg sync.WaitGroup

	for i, de := range kept {
		full := filepath.Join(path, de.Name())
		name := de.Name()
		build := func(i int, full, name string) {
			entries[i], errs[i] = b.d.buildEntry(full, name, ign, b.emit, b.buildDir)
		}
		select {
		case b.sem <- struct{}{}:
			wg.Add(1)
			go func(i int, full, name string) {
				defer wg.Done()
				defer func() { <-b.sem }()
				build(i, full, name)
			}(i, full, name)
		default:
			build(i, full, name)
		}
	}
	wg.Wait()

	db := fstree.NewDirBuilder(b.d.ic)
	for i, e := range entries {
		if errs[i] != nil {
			return key.Key{}, errs[i]
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}
