package packstore

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/draganm/rebma/key"
	"golang.org/x/sys/unix"
)

// A writer owns an active segment by holding an exclusive flock on its data
// file, from the moment it takes the segment until Close or the seal. Nothing
// is locked at Open, so a store that only reads never keeps a segment from
// the next writer. At its first write a store adopts before it creates: it
// takes the largest active segment nobody holds, and makes a new one only
// when every one is taken. Serial writers therefore keep filling one segment
// to the segment size, as a single process always did; the number of active
// segments is bounded by the peak number of simultaneous writers.

// tryLock takes an exclusive flock without waiting.
func tryLock(f *os.File) (bool, error) {
	for {
		switch err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err {
		case nil:
			return true, nil
		case unix.EWOULDBLOCK:
			return false, nil
		case unix.EINTR:
		default:
			return false, err
		}
	}
}

// isFileAt reports whether f is still the file at path: between a listing
// and a lock, a segment can be sealed (renamed) or reaped.
func isFileAt(f *os.File, path string) bool {
	a, err := f.Stat()
	if err != nil {
		return false
	}
	b, err := os.Stat(path)
	return err == nil && os.SameFile(a, b)
}

// ensureActiveLocked makes sure the store owns an active segment to append
// to. Called under appendMu.
func (s *Store) ensureActiveLocked() error {
	if s.active != nil {
		return nil
	}
	ls, err := listSegments(s.dir)
	if err != nil {
		return err
	}
	s.removeStaleTemporaries(ls.tmp)
	cands := make([]segFile, 0, len(ls.active))
	ids := map[string]uint64{}
	for id, sf := range ls.active {
		cands = append(cands, sf)
		ids[sf.path] = id
	}
	slices.SortFunc(cands, func(a, b segFile) int {
		if c := cmp.Compare(b.info.Size(), a.info.Size()); c != 0 {
			return c // the fullest first, so that one fills and seals before the next
		}
		return strings.Compare(a.path, b.path)
	})
	for _, sf := range cands {
		adopted, err := s.adopt(ids[sf.path], sf.path)
		if err != nil {
			return err
		}
		if adopted {
			return nil
		}
	}
	return s.createActive(ls.maxID)
}

// adopt tries to take the active segment at path. It reports false when
// somebody else holds it, when it is gone, or when it turned out to be a
// crashed seal, which is finished here and leaves nothing to append to.
func (s *Store) adopt(id uint64, path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if ok, err := tryLock(f); err != nil || !ok || !isFileAt(f, path) {
		f.Close()
		return false, err
	}
	// The segment is this store's now: only from here on may it be modified.
	res, err := recoverSegment(path)
	if err != nil {
		f.Close()
		return false, err
	}
	if res.sealed {
		// A crash between the footer's write and the rename: finish it. The
		// crash may also have come before the seal's fsync, and a sealed
		// segment is trusted as durable from here on.
		sealedPath := strings.TrimSuffix(path, ".active")
		err := f.Sync()
		if err == nil {
			err = os.Rename(path, sealedPath)
		}
		if err == nil {
			err = s.dirF.Sync()
		}
		if err != nil {
			f.Close()
			return false, err
		}
		os.Remove(path + sidecarSuffix) // a sealed segment indexes itself
		seg, err := openSealed(sealedPath, id)
		f.Close()
		if err != nil {
			return false, err
		}
		s.mu.Lock()
		s.publishSealedLocked(seg)
		s.mu.Unlock()
		return false, nil
	}
	size := res.dataEnd
	if size < int64(len(magicHeader)) {
		// The header never became durable, so nothing in the file was ever
		// acknowledged: start it over. Deliberate and silent.
		res.sidecarEnd, res.missing = 0, nil
		err = f.Truncate(0)
		if err == nil {
			_, err = f.WriteAt(magicHeader, 0)
		}
		size = int64(len(magicHeader))
	} else {
		err = f.Truncate(size)
	}
	if err != nil {
		f.Close()
		return false, err
	}
	a := &activeSegment{id: id, path: path, f: f, size: size, index: res.index, sc: openOwnedSidecar(path, res)}
	s.mu.Lock()
	s.structEpoch++
	s.dropForeignLocked(id)
	s.active = a
	s.mu.Unlock()
	return true, nil
}

// createActive makes a new active segment with an id above every one in the
// directory. The id is claimed by creating the segment's temporary name
// exclusively; the file is locked, checked, written and only then renamed to
// the name other stores look for, so nobody ever sees — or adopts — a
// segment that is not ready and owned.
func (s *Store) createActive(maxID uint64) error {
	for id := max(s.nextID, maxID+1); ; id++ {
		final := filepath.Join(s.dir, segmentName(id, activeSuffix))
		tmp := final + tmpSuffix
		f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue // another creator is at this id
		}
		if err != nil {
			return err
		}
		abandon := func() {
			f.Close()
			os.Remove(tmp)
		}
		// A store clearing away what crashes left behind may have taken the
		// file for such a leftover in the instant before this lock: it holds
		// the lock, or it has already removed the file and let go. Either
		// way the file is lost; claim the next id.
		if ok, err := tryLock(f); err != nil || !ok || !isFileAt(f, tmp) {
			f.Close()
			if err != nil {
				return err
			}
			continue
		}
		// The temporary name is free again as soon as its creator renames
		// it, so holding it does not prove the id is new: look.
		if taken, err := segmentExists(s.dir, id); err != nil || taken {
			abandon()
			if err != nil {
				return err
			}
			continue
		}
		if _, err = f.WriteAt(magicHeader, 0); err == nil {
			err = f.Sync()
		}
		if err == nil {
			err = os.Rename(tmp, final)
		}
		if err == nil {
			err = s.dirF.Sync()
		}
		if err != nil {
			os.Remove(final) // while the lock still holds: nobody may adopt a segment that is on its way out
			abandon()
			return err
		}
		sc, _ := createSidecar(final + sidecarSuffix) // nil on failure: a segment works without one
		a := &activeSegment{id: id, path: final, f: f, size: int64(len(magicHeader)), index: make(map[key.Key]activeLoc), sc: sc}
		s.nextID = id + 1
		s.mu.Lock()
		s.structEpoch++
		s.active = a
		s.mu.Unlock()
		return nil
	}
}

func segmentExists(dir string, id uint64) (bool, error) {
	for _, suffix := range []string{activeSuffix, sealedSuffix} {
		_, err := os.Stat(filepath.Join(dir, segmentName(id, suffix)))
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

// removeStaleTemporaries deletes what crashed creations left behind. One that
// is locked is somebody's creation in progress.
func (s *Store) removeStaleTemporaries(names []string) {
	for _, name := range names {
		path := filepath.Join(s.dir, name)
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			continue
		}
		if ok, _ := tryLock(f); ok && isFileAt(f, path) {
			os.Remove(path)
		}
		f.Close()
	}
}

// lockForeign takes every active segment this store does not own, for an
// operation that is about to delete them. It fails, holding nothing, if one
// is held by a live writer, which would go on appending to a file that is
// gone.
func (s *Store) lockForeign(foreign []*foreignActive) ([]*os.File, error) {
	var held []*os.File
	release := func() {
		for _, f := range held {
			f.Close()
		}
	}
	for _, fa := range foreign {
		f, err := os.OpenFile(fa.path, os.O_RDWR, 0)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			release()
			return nil, err
		}
		ok, err := tryLock(f)
		if err != nil || !ok {
			f.Close()
			release()
			if err == nil {
				err = fmt.Errorf("packstore: segment %016x is being written to by another store", fa.id)
			}
			return nil, err
		}
		held = append(held, f)
	}
	return held, nil
}

// sealIdleLocked seals every active segment that no writer holds. A small
// store's segments never fill, and the process that wrote them may be long
// gone: without this nothing in them could ever be collected. A segment a
// live writer holds is left alone; an active segment is never a victim.
// Called by Compact under appendMu, after it sealed the store's own segment.
func (s *Store) sealIdleLocked() error {
	if a := s.active; a != nil {
		// Still owned, so empty: sealing left it alone. Let go of it for the
		// pass; it is on disk for whoever writes next.
		a.sc.close()
		a.f.Close()
		s.mu.Lock()
		s.structEpoch++
		s.active = nil
		// The segment is now neither this store's nor in its view of the
		// others', and whoever takes it next changes nothing in the
		// directory: the next lookup that misses has to list it.
		s.dirMtime = time.Time{}
		s.mu.Unlock()
	}
	ls, err := listSegments(s.dir)
	if err != nil {
		return err
	}
	ids := make([]uint64, 0, len(ls.active))
	for id := range ls.active {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b uint64) int { // the fullest first: the empty ones cannot be sealed
		return cmp.Or(cmp.Compare(ls.active[b].info.Size(), ls.active[a].info.Size()), cmp.Compare(a, b))
	})
	for _, id := range ids {
		adopted, err := s.adopt(id, ls.active[id].path)
		if err != nil {
			return err
		}
		if !adopted {
			continue
		}
		if err := s.sealActiveLocked(); err != nil {
			return err
		}
		if s.active != nil {
			return nil // an empty one: the pass appends its survivors to it
		}
	}
	return nil
}
