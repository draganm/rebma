// Package ingest builds content-addressed filesystem trees from a local
// directory or a single regular file: it walks the source, applies
// .amberignore filtering, splits file content by content-defined chunking,
// and streams every built object (children before parents) to the consumer.
// Objects exposes the raw object stream for consumers that route objects
// themselves; Dir writes the stream straight into a packstore.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"runtime"

	"github.com/draganm/rebma/amberignore"
	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
)

// DefaultItemBits is the item-chunker bit width used when Opts leaves it zero:
// directory leaves and file index nodes average 2^7 entries.
const DefaultItemBits = 7

// DefaultXattrInlineMax is the inline-xattr cap used when Opts leaves it zero:
// encoded xattrs larger than this many bytes spill to an XattrSet object.
const DefaultXattrInlineMax = 256

// Progress receives build-progress events. Implementations must be safe for
// concurrent use; a nil Progress disables reporting. Totals for sizing a
// progress display come from Scan.
type Progress interface {
	// FileDone records one regular file fully chunked.
	FileDone()
	// AddBytes records n source-content bytes chunked.
	AddBytes(n int)
}

// ChunkOpts sets the content-defined-chunking parameters. The zero value
// selects the library defaults; the parameters determine every object key, so
// two builds of the same tree agree only when their ChunkOpts agree.
type ChunkOpts struct {
	Byte           *chunkers.ByteOpts // byte-chunker sizes; nil selects chunkers.Default{Min,Normal,Max}Size
	ItemBits       int                // item chunker average run = 2^bits; 0 selects DefaultItemBits
	XattrInlineMax int                // xattr spill threshold in bytes; 0 selects DefaultXattrInlineMax
}

// Opts configures a tree build.
type Opts struct {
	// Jobs bounds the concurrent build workers; <1 selects GOMAXPROCS.
	Jobs int
	// Chunk sets the chunking parameters.
	Chunk ChunkOpts
	// NoIgnore disables .amberignore filtering (which only applies to
	// directory builds).
	NoIgnore bool
	// Progress, when non-nil, receives build-progress events.
	Progress Progress
	// Exclude lists names directly under the root that are never ingested,
	// whatever NoIgnore says. It applies to the root directory only; the
	// same name deeper in the tree is ingested normally.
	Exclude []string
}

// jobs resolves the effective worker count.
func (o Opts) jobs() int {
	if o.Jobs < 1 {
		return runtime.GOMAXPROCS(0)
	}
	return o.Jobs
}

// excludeSet turns Opts.Exclude into a set; nil when empty.
func (o Opts) excludeSet() map[string]bool {
	if len(o.Exclude) == 0 {
		return nil
	}
	m := make(map[string]bool, len(o.Exclude))
	for _, n := range o.Exclude {
		m[n] = true
	}
	return m
}

// driver builds a driver from the chunking options.
func (o Opts) driver() *driver {
	bits := o.Chunk.ItemBits
	if bits == 0 {
		bits = DefaultItemBits
	}
	inlineMax := o.Chunk.XattrInlineMax
	if inlineMax == 0 {
		inlineMax = DefaultXattrInlineMax
	}
	return &driver{
		ic:             chunkers.NewItemChunker(bits),
		byteOpts:       o.Chunk.Byte,
		xattrInlineMax: inlineMax,
		progress:       o.Progress,
	}
}

// Objects builds the tree at path — a directory, or a single regular file —
// and returns the stream of every CAS object it produces plus a pointer to
// the root key, which is set once the stream has been consumed to completion
// without error. A directory root is a DirNode; a file root is the file's
// content key (Blob or FileNode).
//
// Object order is unspecified — the store is a flat content-addressed bag and
// dedups by key — but per-file chunk order and per-directory entry order are
// preserved, so every object's key, and the root, are deterministic functions
// of the source tree and the chunking options.
func Objects(path string, opts Opts) (iter.Seq2[fstree.Object, error], *key.Key, error) {
	isDir, err := statPath(path)
	if err != nil {
		return nil, nil, err
	}
	var ign *amberignore.Matcher
	if isDir && !opts.NoIgnore {
		ign, err = amberignore.Root(path)
		if err != nil {
			return nil, nil, err
		}
	}
	d := opts.driver()
	jobs := opts.jobs()

	var buildRoot func(fstree.Emit) (key.Key, error)
	if isDir {
		buildRoot = func(emit fstree.Emit) (key.Key, error) {
			b := &pbuilder{d: d, emit: emit, sem: make(chan struct{}, jobs), root: path, exclude: opts.excludeSet()}
			return b.buildDir(path, ign, emit)
		}
	} else {
		buildRoot = func(emit fstree.Emit) (key.Key, error) {
			k, err := d.buildFile(path, emit)
			if err == nil {
				d.fileDone()
			}
			return k, err
		}
	}
	root := new(key.Key)
	return objects(buildRoot, jobs*2, root), root, nil
}

// Dir builds the tree at path — a directory, or a single regular file — and
// stores every object in st via WriteParallel, returning the resolved root
// key and the write stats.
func Dir(st *packstore.Store, path string, opts Opts) (key.Key, packstore.WriteStats, error) {
	seq, root, err := Objects(path, opts)
	if err != nil {
		return key.Key{}, packstore.WriteStats{}, err
	}
	stats, err := st.WriteParallel(func(yield func(packstore.Object, error) bool) {
		for o, err := range seq {
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, err) {
				return
			}
		}
	}, packstore.WriteOpts{Writers: opts.Jobs})
	if err != nil {
		return key.Key{}, stats, err
	}
	return *root, stats, nil
}

// statPath reports whether path is a directory. It errors if path does not
// exist or is neither a regular file nor a directory (symlinks are followed).
func statPath(path string) (isDir bool, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	switch {
	case info.IsDir():
		return true, nil
	case info.Mode().IsRegular():
		return false, nil
	default:
		return false, fmt.Errorf("%s is neither a regular file nor a directory", path)
	}
}

// errStopped aborts the tree walk when the consumer stops pulling from the
// object iterator early. It never escapes objects.
var errStopped = errors.New("ingest: consumer stopped")

// objects returns an iterator over every CAS object produced by buildRoot.
// buildRoot receives the (concurrency-safe) emit sink and returns the
// resolved root key. Built objects stream to the consumer through a buffered
// channel of bufSize, so production and consumption overlap.
//
// Once the build completes successfully the resolved root key is written to
// *root; a build error is yielded to the consumer instead.
func objects(buildRoot func(fstree.Emit) (key.Key, error), bufSize int, root *key.Key) iter.Seq2[fstree.Object, error] {
	if bufSize < 1 {
		bufSize = 1
	}
	return func(yield func(fstree.Object, error) bool) {
		// ctx is cancelled only when the consumer stops pulling early (yield
		// returns false). Build errors propagate through the normal return
		// path instead, so they are never masked by stop signals.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan fstree.Object, bufSize)
		var buildErr error

		go func() {
			defer close(ch)
			emit := func(o fstree.Object) error {
				select {
				case ch <- o:
					return nil
				case <-ctx.Done():
					return errStopped
				}
			}
			rk, err := buildRoot(emit)
			if err != nil {
				// errStopped means the consumer quit; not a real error.
				if !errors.Is(err, errStopped) {
					buildErr = err
				}
				return
			}
			*root = rk
		}()

		for o := range ch {
			if !yield(o, nil) {
				cancel() // unblock producers parked on the channel send
				for range ch {
				}
				return
			}
		}
		if buildErr != nil {
			yield(fstree.Object{}, buildErr)
		}
	}
}
