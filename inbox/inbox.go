package inbox

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/zeebo/blake3"
)

// Inbox receives packs, persists them durably, and drains them into a
// packstore from a worker pool. It is safe for concurrent use.
type Inbox struct {
	dir     string
	tmpDir  string
	failDir string
	store   *packstore.Store
	log     *slog.Logger
	gate    func() func() // see WithGate

	mu     sync.Mutex
	cond   *sync.Cond
	work   []workItem
	groups map[key.Key]int // root -> count of unprocessed entries
	closed bool

	wg sync.WaitGroup
}

type workItem struct {
	name string
	root key.Key
}

// Open prepares the inbox directory tree, recovers entries left by a previous
// run, and starts `workers` processing goroutines. workers <= 0 means
// runtime.GOMAXPROCS(0). A nil log discards.
// Option tweaks Open.
type Option func(*Inbox)

// WithGate brackets every entry's store write with gate: acquire before the
// WriteParallel, release after. The GC collector's BeginWrite is the intended
// gate — it keeps a drain's dedup hits from racing a sweep's pack removal.
func WithGate(gate func() (done func())) Option {
	return func(ib *Inbox) { ib.gate = gate }
}

func Open(dir string, store *packstore.Store, workers int, log *slog.Logger, opts ...Option) (*Inbox, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	ib := &Inbox{
		dir:     dir,
		tmpDir:  filepath.Join(dir, "tmp"),
		failDir: filepath.Join(dir, "failed"),
		store:   store,
		log:     log,
		groups:  map[key.Key]int{},
	}
	ib.cond = sync.NewCond(&ib.mu)
	for _, o := range opts {
		o(ib)
	}
	for _, d := range []string{ib.dir, ib.tmpDir, ib.failDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := ib.recover(); err != nil {
		return nil, err
	}
	for range workers {
		ib.wg.Add(1)
		go ib.processLoop()
	}
	return ib, nil
}

// Stage writes meta and streams body into a fresh tmp file, returning the tmp
// path, blake3 of the body bytes (only the body feeds the hash), and the body
// length. The caller authorizes the request against the hash, then calls
// Commit or Discard.
func (ib *Inbox) Stage(meta Meta, body io.Reader) (tmpPath string, bodyHash []byte, n int64, err error) {
	f, err := os.CreateTemp(ib.tmpDir, "stage-*")
	if err != nil {
		return "", nil, 0, err
	}
	tmpPath = f.Name()
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(tmpPath)
		}
	}()
	if err := writeMetaHeader(f, meta); err != nil {
		return "", nil, 0, err
	}
	h := blake3.New()
	n, err = io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		return "", nil, 0, err
	}
	if err := f.Sync(); err != nil {
		return "", nil, 0, err
	}
	if err := f.Close(); err != nil {
		return "", nil, 0, err
	}
	committed = true
	return tmpPath, h.Sum(nil), n, nil
}

// Discard removes a staged tmp file (authorization failed or oversize).
func (ib *Inbox) Discard(tmpPath string) {
	_ = os.Remove(tmpPath)
}

// Commit publishes a staged tmp file under its content-addressed name and
// enqueues it. It is idempotent: if an entry with the same body already exists,
// the tmp file is discarded and added is false. root updates the barrier
// accounting and must equal the root staged into the file.
func (ib *Inbox) Commit(tmpPath string, bodyHash []byte, root key.Key) (added bool, err error) {
	name := hex.EncodeToString(bodyHash) + ".pack"
	dst := filepath.Join(ib.dir, name)
	switch _, statErr := os.Stat(dst); {
	case statErr == nil:
		ib.Discard(tmpPath)
		return false, nil
	case !errors.Is(statErr, os.ErrNotExist):
		return false, statErr
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return false, err
	}
	if err := syncDir(ib.dir); err != nil {
		return false, err
	}
	ib.mu.Lock()
	ib.groups[root]++
	ib.work = append(ib.work, workItem{name: name, root: root})
	ib.cond.Broadcast()
	ib.mu.Unlock()
	return true, nil
}

// WaitFor blocks until no entries tagged with root remain unprocessed. With an
// empty group it returns immediately.
func (ib *Inbox) WaitFor(root key.Key) {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	for ib.groups[root] > 0 {
		ib.cond.Wait()
	}
}

// Close stops accepting new work, drains what is already queued, and waits for
// the processing goroutines to exit. Staged-but-uncommitted tmp files are left
// for the next Open to sweep.
func (ib *Inbox) Close() error {
	ib.mu.Lock()
	ib.closed = true
	ib.cond.Broadcast()
	ib.mu.Unlock()
	ib.wg.Wait()
	return nil
}

// recover sweeps partial transfers and enqueues committed entries from a
// previous run.
func (ib *Inbox) recover() error {
	tmps, err := os.ReadDir(ib.tmpDir)
	if err != nil {
		return err
	}
	for _, e := range tmps {
		_ = os.Remove(filepath.Join(ib.tmpDir, e.Name()))
	}
	entries, err := os.ReadDir(ib.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		root, err := ib.readRoot(e.Name())
		if err != nil {
			ib.log.Error("inbox: unreadable entry on recovery, quarantining", "name", e.Name(), "error", err)
			_ = os.Rename(filepath.Join(ib.dir, e.Name()), filepath.Join(ib.failDir, e.Name()))
			continue
		}
		ib.groups[root]++
		ib.work = append(ib.work, workItem{name: e.Name(), root: root})
	}
	return nil
}

// readRoot reads just the meta header of an entry and returns its root key.
func (ib *Inbox) readRoot(name string) (key.Key, error) {
	f, err := os.Open(filepath.Join(ib.dir, name))
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()
	m, err := readMetaHeader(f)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(m.Root)
}

func (ib *Inbox) processLoop() {
	defer ib.wg.Done()
	for {
		ib.mu.Lock()
		for len(ib.work) == 0 && !ib.closed {
			ib.cond.Wait()
		}
		if len(ib.work) == 0 && ib.closed {
			ib.mu.Unlock()
			return
		}
		item := ib.work[0]
		ib.work = ib.work[1:]
		ib.mu.Unlock()

		ib.process(item.name)

		ib.mu.Lock()
		if n := ib.groups[item.root]; n > 0 {
			if n == 1 {
				delete(ib.groups, item.root)
			} else {
				ib.groups[item.root] = n - 1
			}
		}
		ib.cond.Broadcast()
		ib.mu.Unlock()
	}
}

// process ingests one entry into the store. On success the file is removed; on
// a decode/verify error it is quarantined under failed/.
func (ib *Inbox) process(name string) {
	path := filepath.Join(ib.dir, name)
	f, err := os.Open(path)
	if err != nil {
		ib.log.Error("inbox: opening entry failed", "name", name, "error", err)
		return
	}
	if _, err := readMetaHeader(f); err != nil {
		f.Close()
		ib.quarantine(name, err)
		return
	}
	rd := amberpack.NewReader(f) // positioned at the body
	seq := func(yield func(packstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(packstore.Object{}, err)
				return
			}
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	var werr error
	if ib.gate != nil {
		done := ib.gate()
		_, werr = ib.store.WriteParallel(seq, packstore.WriteOpts{Verify: true})
		done()
	} else {
		_, werr = ib.store.WriteParallel(seq, packstore.WriteOpts{Verify: true})
	}
	f.Close()
	if werr != nil {
		ib.quarantine(name, werr)
		return
	}
	if err := os.Remove(path); err != nil {
		ib.log.Error("inbox: removing processed entry failed", "name", name, "error", err)
	}
}

func (ib *Inbox) quarantine(name string, cause error) {
	ib.log.Error("inbox: entry failed processing, quarantining", "name", name, "error", cause)
	if err := os.Rename(filepath.Join(ib.dir, name), filepath.Join(ib.failDir, name)); err != nil {
		ib.log.Error("inbox: quarantine rename failed", "name", name, "error", err)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
