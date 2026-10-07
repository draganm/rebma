package ingest

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/draganm/rebma/amberignore"
)

// Scan walks the directory tree at dir concurrently (ReadDir + Lstat only, no
// content reads) and returns the number of regular files and the total size
// of their content — the totals a progress display is sized by. Only
// regular-file bytes are counted, since only regular files are read during a
// build; symlinks, dirs and special files contribute nothing. The first
// stat/read error aborts the scan.
//
// When noIgnore is false, .amberignore filtering rooted at dir is applied
// exactly as a build applies it, so the totals match what the build reads.
//
// The fan-out mirrors the parallel build: each entry may run on a pooled
// goroutine, but when the pool is full the work runs inline on the current
// goroutine, so the recursion cannot deadlock waiting on a slot held by a
// descendant.
func Scan(dir string, noIgnore bool, jobs int) (files int64, bytes int64, err error) {
	var ign *amberignore.Matcher
	if !noIgnore {
		if ign, err = amberignore.Root(dir); err != nil {
			return 0, 0, err
		}
	}
	return scanTree(dir, ign, jobs)
}

// ScanWith is Scan with every relevant option of opts applied: NoIgnore,
// Jobs and Exclude, so that the totals match what Objects(dir, opts) reads.
func ScanWith(dir string, opts Opts) (files int64, bytes int64, err error) {
	var ign *amberignore.Matcher
	if !opts.NoIgnore {
		if ign, err = amberignore.Root(dir); err != nil {
			return 0, 0, err
		}
	}
	s := &scanner{sem: make(chan struct{}, opts.jobs()), root: dir, exclude: opts.excludeSet()}
	s.walk(dir, ign)
	if e := s.err(); e != nil {
		return 0, 0, e
	}
	return s.files.Load(), s.bytes.Load(), nil
}

// scanTree implements Scan over an explicit matcher (nil ingests everything).
func scanTree(dir string, ign *amberignore.Matcher, jobs int) (files int64, bytes int64, err error) {
	if jobs < 1 {
		jobs = 1
	}
	s := &scanner{sem: make(chan struct{}, jobs)}
	s.walk(dir, ign)
	if e := s.err(); e != nil {
		return 0, 0, e
	}
	return s.files.Load(), s.bytes.Load(), nil
}

type scanner struct {
	files atomic.Int64
	bytes atomic.Int64
	sem   chan struct{}

	mu       sync.Mutex
	firstErr error

	// root and exclude implement Opts.Exclude for ScanWith.
	root    string
	exclude map[string]bool
}

func (s *scanner) setErr(e error) {
	s.mu.Lock()
	if s.firstErr == nil {
		s.firstErr = e
	}
	s.mu.Unlock()
}

func (s *scanner) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

func (s *scanner) walk(dir string, ign *amberignore.Matcher) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		s.setErr(err)
		return
	}
	var wg sync.WaitGroup
	for _, de := range ents {
		if dir == s.root && s.exclude[de.Name()] {
			continue
		}
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
		full := filepath.Join(dir, de.Name())
		name := de.Name()
		do := func(full, name string) {
			info, err := os.Lstat(full)
			if err != nil {
				s.setErr(err)
				return
			}
			switch info.Mode() & os.ModeType {
			case os.ModeDir:
				sub, err := ign.Descend(full, name)
				if err != nil {
					s.setErr(err)
					return
				}
				s.walk(full, sub)
			case 0: // regular file
				s.files.Add(1)
				s.bytes.Add(info.Size())
			default:
				// symlink, device, socket, fifo — not read during ingest.
			}
		}
		select {
		case s.sem <- struct{}{}:
			wg.Add(1)
			go func(full, name string) {
				defer wg.Done()
				defer func() { <-s.sem }()
				do(full, name)
			}(full, name)
		default:
			do(full, name)
		}
	}
	wg.Wait()
}
