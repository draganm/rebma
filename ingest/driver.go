package ingest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/draganm/rebma/amberignore"
	"github.com/draganm/rebma/cborx"
	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"golang.org/x/sys/unix"
)

type driver struct {
	ic             chunkers.ItemChunker
	byteOpts       *chunkers.ByteOpts
	xattrInlineMax int
	progress       Progress // nil disables reporting
}

// fileDone forwards to the progress sink, if any.
func (d *driver) fileDone() {
	if d.progress != nil {
		d.progress.FileDone()
	}
}

// addBytes forwards to the progress sink, if any.
func (d *driver) addBytes(n int) {
	if d.progress != nil {
		d.progress.AddBytes(n)
	}
}

// buildDir builds the directory at path and returns its root key, emitting every
// object in its subtree (children before parents). Entries excluded by ign are
// skipped; excluded directories are pruned without being read.
func (d *driver) buildDir(path string, ign *amberignore.Matcher, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	db := fstree.NewDirBuilder(d.ic)
	for _, de := range ents {
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
		full := filepath.Join(path, de.Name())
		e, err := d.buildEntry(full, de.Name(), ign, emit, d.buildDir)
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

// buildEntry produces the directory entry for one path, recursing into files and
// subdirectories (and emitting their objects) and reading inline metadata for
// links and special files. buildDir is the function used to build a
// subdirectory: d.buildDir for the sequential pack walk, or the parallel
// builder's buildDir for concurrent ingestion.
func (d *driver) buildEntry(full, name string, ign *amberignore.Matcher, emit fstree.Emit, buildDir func(string, *amberignore.Matcher, fstree.Emit) (key.Key, error)) (fstree.Entry, error) {
	info, err := os.Lstat(full)
	if err != nil {
		return fstree.Entry{}, err
	}
	m := entryMeta(info)
	e := fstree.Entry{
		Name:  []byte(name),
		Mode:  m.Mode,
		UID:   m.UID,
		GID:   m.GID,
		Mtime: m.Mtime,
	}

	switch m.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		ck, err := d.buildFile(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
		d.fileDone()
	case unix.S_IFDIR:
		sub, err := ign.Descend(full, name)
		if err != nil {
			return fstree.Entry{}, err
		}
		ck, err := buildDir(full, sub, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFLNK:
		target, err := os.Readlink(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.LinkTarget = []byte(target)
	case unix.S_IFCHR, unix.S_IFBLK:
		major, minor := deviceNumbers(info)
		e.Rdev = []uint64{major, minor}
	case unix.S_IFIFO, unix.S_IFSOCK:
		// no payload key
	default:
		return fstree.Entry{}, fmt.Errorf("%s: unsupported file type %#o", full, m.Mode&unix.S_IFMT)
	}

	// Extended attributes (skip symlinks).
	if m.Mode&unix.S_IFMT != unix.S_IFLNK {
		xattrs, err := readXattrs(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		if len(xattrs) > 0 {
			enc := cborx.EncodeXattrs(xattrs)
			if len(enc) <= d.xattrInlineMax {
				e.XattrsIn = enc
			} else {
				obj, err := fstree.EncodeXattrSet(xattrs)
				if err != nil {
					return fstree.Entry{}, err
				}
				if err := emit(obj); err != nil {
					return fstree.Entry{}, err
				}
				e.XattrsKey = obj.Key[:]
			}
		}
	}
	return e, nil
}

// buildFile chunks a regular file into Blobs (emitting each), builds its FileNode
// index, and returns the file's root key.
func (d *driver) buildFile(full string, emit fstree.Emit) (key.Key, error) {
	f, err := os.Open(full)
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()

	ib := fstree.NewFileIndexBuilder(d.ic)
	saw := false
	err = chunkers.SplitBytes(f, d.byteOpts, func(chunk []byte) error {
		d.addBytes(len(chunk))
		saw = true
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}
		return ib.AddChild(emit, obj.Key, nil)
	})
	if err != nil {
		return key.Key{}, err
	}
	if !saw {
		obj, err := fstree.EncodeBlob([]byte{})
		if err != nil {
			return key.Key{}, err
		}
		if err := emit(obj); err != nil {
			return key.Key{}, err
		}
		if err := ib.AddChild(emit, obj.Key, nil); err != nil {
			return key.Key{}, err
		}
	}
	return ib.Finish(emit)
}
