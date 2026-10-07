// amber-bench: an ingest → delete → gc benchmark for amber-store.
//
// The workload is 1000 references over ~50 GiB of unique data, interleaved
// into two classes so that deleting 700 of them leaves ~30 GiB: "kept" refs
// (i%10 < 3) carry 100 MiB of fresh random data each, "deleted" refs
// (i%10 >= 3) carry 30 MiB. Every ref i > 0 also contains whole-file
// copies (reflinks, so they cost no disk) of ~1/3 of its fresh size taken
// from ref i-1, so ~25 % of the ingested bytes are duplicates already in the
// store — and, because kept refs copy from deleted neighbours too, the
// bytes the deleted refs "own" and the bytes a collection can actually
// reclaim differ. Files are 256 KiB–4 MiB; content is seeded, so a rerun
// produces the identical dataset and pack layout.
//
// Phases (-phase, default all): gen writes the dataset; ingest stores every
// ref in-process the way the CLI and a daemon do (ingest.Dir, then the
// collector's PrepareRef) and times each; delete removes the 700 through
// ReleaseRef; gc runs the real CLI — `gc run` under policy, then a forced
// `--garbage 0` pass — with `gc status` and du snapshots around each step;
// verify runs fstree.CheckComplete on every surviving ref and restores a
// sample through the CLI, comparing it byte for byte with the source; report
// prints the summary from the results file. Each phase appends to -out, so
// phases can be rerun individually.
//
// The dataset plus the store need ~2× the unique size on disk. At full
// scale the run is a few minutes on an SSD; -refs and -scale shrink it
// (-refs 30 -scale 0.1 -segment 4194304 is a seconds-long smoke test that
// still seals and reaps packs).
//
//	go build -o /tmp/amber-store ./cmd/amber-store
//	go run ./cmd/amber-bench -data /tmp/bench/data -store /tmp/bench/store \
//	    -restore /tmp/bench/restore -bin /tmp/amber-store -out /tmp/bench/results.json
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/gc"
	"github.com/draganm/rebma/ingest"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
	"golang.org/x/sys/unix"
)

const (
	MiB     = 1 << 20
	minFile = 256 << 10
	maxFile = 4 * MiB
	seed    = 20260824
)

// config is the command line.
type config struct {
	data    string
	store   string
	bin     string
	out     string
	restore string
	refs    int
	scale   float64
	segment int64
}

func main() {
	cfg := &config{}
	flag.StringVar(&cfg.data, "data", "", "dataset directory (written by gen)")
	flag.StringVar(&cfg.store, "store", "", "store directory")
	flag.StringVar(&cfg.bin, "bin", "", "amber-store CLI binary for the gc and verify phases (default: amber-store in PATH)")
	flag.StringVar(&cfg.out, "out", "results.json", "results file; every phase reads and extends it")
	flag.StringVar(&cfg.restore, "restore", "", "scratch directory for verify's sample restores (empty: skip them)")
	flag.IntVar(&cfg.refs, "refs", 1000, "number of references")
	flag.Float64Var(&cfg.scale, "scale", 1.0, "multiplier on every per-ref size")
	flag.Int64Var(&cfg.segment, "segment", packstore.DefaultSegmentSize, "pack segment size in bytes (harness and CLI)")
	phase := flag.String("phase", "all", "gen|ingest|delete|gc|verify|report|all")
	flag.Parse()
	if err := run(cfg, *phase); err != nil {
		fmt.Fprintln(os.Stderr, "amber-bench:", err)
		os.Exit(1)
	}
}

func run(cfg *config, phase string) error {
	if phase != "report" && (cfg.data == "" || cfg.store == "") {
		return errors.New("-data and -store are required")
	}
	if cfg.bin == "" {
		if p, err := exec.LookPath("amber-store"); err == nil {
			cfg.bin = p
		}
	}
	res, err := loadResults(cfg.out)
	if err != nil {
		return err
	}
	res.Refs, res.Scale = cfg.refs, cfg.scale
	switch phase {
	case "gen":
		return phaseGen(cfg, res)
	case "ingest":
		return phaseIngest(cfg, res)
	case "delete":
		return phaseDelete(cfg, res)
	case "gc":
		return phaseGC(cfg, res)
	case "verify":
		return phaseVerify(cfg, res)
	case "report":
		return report(os.Stdout, res)
	case "all":
		steps := []func(*config, *results) error{
			phaseGen, phaseIngest,
			func(cfg *config, res *results) error { return snapshotStore(cfg, res, "after-ingest") },
			phaseDelete,
			func(cfg *config, res *results) error { return snapshotStore(cfg, res, "after-delete") },
			phaseGC, phaseVerify,
		}
		for _, step := range steps {
			if err := step(cfg, res); err != nil {
				return err
			}
		}
		return report(os.Stdout, res)
	}
	return fmt.Errorf("unknown phase %q", phase)
}

// ---------------------------------------------------------------- results

type fileInfo struct {
	Name string
	Size int64
}

type refManifest struct {
	Index   int
	Kept    bool
	Fresh   int64 // bytes of fresh random data
	Shared  int64 // bytes cloned from the previous ref
	Logical int64 // Fresh + Shared: what ingest reads
	Files   []fileInfo
}

type refIngest struct {
	Index       int
	Kept        bool
	Logical     int64
	Fresh       int64
	Shared      int64
	Stored      int
	Deduped     int
	BytesStored int64
	IngestNs    int64 // ingest.Dir
	RefNs       int64 // PrepareRef walk + refstore put + commit
}

type snapshot struct {
	Label        string
	PackstoreKiB int64
	RefsKiB      int64
	ClosuresKiB  int64
	Segments     int // sealed + active
	FreeBytes    uint64
	GCStatus     string // totals lines of `gc status`
}

type cliRun struct {
	Args    []string
	Output  string
	WallNs  int64
	ExitErr string
}

type results struct {
	Refs            int
	Scale           float64
	GenNs           int64
	Manifests       []refManifest `json:",omitempty"`
	Ingest          []refIngest
	IngestWallNs    int64 // first ingest.Dir start → last reference commit
	IngestCloseNs   int64
	DeleteNs        int64
	DeleteN         int
	Snapshots       []snapshot
	GCRuns          []cliRun
	VerifyComplete  int
	VerifyRestoreOK []string
	VerifyErrors    []string
}

func loadResults(path string) (*results, error) {
	res := &results{}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, res); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return res, nil
}

func saveResults(cfg *config, res *results) error {
	b, err := json.MarshalIndent(res, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfg.out, b, 0o644)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05")+" "+format+"\n", args...)
}

func kept(i int) bool                  { return i%10 < 3 }
func refName(i int) string             { return fmt.Sprintf("bench/ref-%04d", i) }
func refDir(cfg *config, i int) string { return filepath.Join(cfg.data, fmt.Sprintf("ref-%04d", i)) }
func (cfg *config) freshTarget(i int) int64 {
	if kept(i) {
		return int64(100 * MiB * cfg.scale)
	}
	return int64(30 * MiB * cfg.scale)
}

// ---------------------------------------------------------------- gen

func phaseGen(cfg *config, res *results) error {
	n := cfg.refs
	logf("gen: %d refs into %s", n, cfg.data)
	if err := os.MkdirAll(cfg.data, 0o755); err != nil {
		return err
	}
	start := time.Now()
	ms := make([]refManifest, n)
	errs := make([]error, n)
	work := make(chan int)
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			buf := make([]byte, MiB)
			for i := range work {
				ms[i], errs[i] = genFresh(cfg, i, buf)
			}
		})
	}
	for i := range n {
		work <- i
	}
	close(work)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	logf("gen: fresh data written in %s; cloning shared files", time.Since(start).Round(time.Second))
	// Shared files: whole-file clones from the previous ref, chosen in a
	// seeded shuffle, never overshooting the target.
	for i := 1; i < n; i++ {
		rng := rand.New(rand.NewPCG(seed+1, uint64(i)))
		target := ms[i].Fresh / 3
		prev := slices.Clone(ms[i-1].Files)
		rng.Shuffle(len(prev), func(a, b int) { prev[a], prev[b] = prev[b], prev[a] })
		var sum int64
		for c, f := range prev {
			if sum+f.Size > target {
				continue
			}
			src := filepath.Join(refDir(cfg, i-1), f.Name)
			name := fmt.Sprintf("c%04d.bin", c)
			if err := cloneFile(src, filepath.Join(refDir(cfg, i), name)); err != nil {
				return fmt.Errorf("clone %s: %w", src, err)
			}
			ms[i].Files = append(ms[i].Files, fileInfo{name, f.Size})
			sum += f.Size
		}
		ms[i].Shared = sum
	}
	var fresh, shared int64
	for i := range ms {
		ms[i].Logical = ms[i].Fresh + ms[i].Shared
		fresh += ms[i].Fresh
		shared += ms[i].Shared
	}
	res.GenNs = int64(time.Since(start))
	res.Manifests = ms
	logf("gen: done in %s: fresh %s, shared %s, logical %s (overlap %.1f%%)",
		time.Since(start).Round(time.Second), human(fresh), human(shared), human(fresh+shared),
		100*float64(shared)/float64(fresh+shared))
	return saveResults(cfg, res)
}

func genFresh(cfg *config, i int, buf []byte) (refManifest, error) {
	rng := rand.New(rand.NewPCG(seed, uint64(i)))
	dir := refDir(cfg, i)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return refManifest{}, err
	}
	m := refManifest{Index: i, Kept: kept(i)}
	remaining := cfg.freshTarget(i)
	for n := 0; remaining > 0; n++ {
		size := min(int64(minFile+rng.IntN(maxFile-minFile+1)), remaining)
		name := fmt.Sprintf("f%04d.bin", n)
		if err := writeRandom(filepath.Join(dir, name), size, uint64(i)<<32|uint64(n), buf); err != nil {
			return m, err
		}
		m.Files = append(m.Files, fileInfo{name, size})
		m.Fresh += size
		remaining -= size
	}
	return m, nil
}

// writeRandom fills path with size bytes of an xorshift64* stream seeded by
// s: incompressible, so stored bytes track ingested bytes.
func writeRandom(path string, size int64, s uint64, buf []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	x := (s+1)*0x9E3779B97F4A7C15 | 1
	for size > 0 {
		n := int(min(int64(len(buf)), size))
		for off := 0; off+8 <= n; off += 8 {
			x ^= x >> 12
			x ^= x << 25
			x ^= x >> 27
			binary.LittleEndian.PutUint64(buf[off:], x*0x2545F4914F6CDD1D)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			f.Close()
			return err
		}
		size -= int64(n)
	}
	return f.Close()
}

// copyFile is the reflink fallback.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// ---------------------------------------------------------------- store

// openAll opens the store exactly as cmd/amber-store does, plus the collector.
func openAll(cfg *config, opts gc.Options) (*packstore.Store, *refstore.Store, *gc.Collector, error) {
	objects, err := packstore.Open(filepath.Join(cfg.store, "packstore"),
		packstore.WithSync(true), packstore.WithSegmentSize(cfg.segment))
	if err != nil {
		return nil, nil, nil, err
	}
	refs, err := refstore.Open(filepath.Join(cfg.store, "refs"), true)
	if err != nil {
		objects.Close()
		return nil, nil, nil, err
	}
	coll, err := gc.Open(filepath.Join(cfg.store, "closures"), objects, refs, opts)
	if err != nil {
		refs.Close()
		objects.Close()
		return nil, nil, nil, err
	}
	return objects, refs, coll, nil
}

func closeAll(objects *packstore.Store, refs *refstore.Store, coll *gc.Collector) error {
	return errors.Join(coll.Close(), refs.Close(), objects.Close())
}

// putRef mirrors cmd/amber-store's reference PUT.
func putRef(coll *gc.Collector, refs *refstore.Store, name string, root key.Key, raw []byte) error {
	var old *key.Key
	if prev, err := refs.Get(name); err == nil {
		prevRef, err := reference.Decode(prev)
		if err != nil {
			return err
		}
		k, err := key.Parse(prevRef.Key)
		if err != nil {
			return err
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}
	commit, abort, err := coll.PrepareRef(root)
	if err != nil {
		return err
	}
	if err := refs.Put(name, raw); err != nil {
		abort()
		return err
	}
	commit()
	if old != nil {
		return coll.ReleaseRef(*old)
	}
	return nil
}

// rmRef mirrors cmd/amber-store's `ref rm`.
func rmRef(coll *gc.Collector, refs *refstore.Store, name string) error {
	root, err := rootOf(refs, name)
	if err != nil {
		return err
	}
	if err := refs.Delete(name); err != nil {
		return err
	}
	return coll.ReleaseRef(root)
}

func rootOf(refs *refstore.Store, name string) (key.Key, error) {
	raw, err := refs.Get(name)
	if err != nil {
		return key.Key{}, err
	}
	ref, err := reference.Decode(raw)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(ref.Key)
}

// ---------------------------------------------------------------- ingest

func phaseIngest(cfg *config, res *results) error {
	if len(res.Manifests) != cfg.refs {
		return fmt.Errorf("ingest: manifests for %d refs, want %d (run gen first)", len(res.Manifests), cfg.refs)
	}
	objects, refs, coll, err := openAll(cfg, gc.Options{})
	if err != nil {
		return err
	}
	opts := ingest.Opts{
		Jobs: runtime.GOMAXPROCS(0),
		Chunk: ingest.ChunkOpts{ // the CLI's defaults
			Byte:           &chunkers.ByteOpts{MinSize: 32 << 10, NormalSize: 128 << 10, MaxSize: 256 << 10},
			ItemBits:       ingest.DefaultItemBits,
			XattrInlineMax: ingest.DefaultXattrInlineMax,
		},
	}
	logf("ingest: %d refs, jobs=%d", cfg.refs, opts.Jobs)
	res.Ingest = res.Ingest[:0]
	start := time.Now()
	var logical, stored int64
	for i := range cfg.refs {
		m := res.Manifests[i]
		t0 := time.Now()
		root, ws, err := ingest.Dir(objects, refDir(cfg, i), opts)
		if err != nil {
			closeAll(objects, refs, coll)
			return fmt.Errorf("ingest ref %d: %w", i, err)
		}
		t1 := time.Now()
		rec := reference.Reference{Name: refName(i), Key: root[:], CreatedAt: time.Now().UnixNano()}
		raw, err := rec.Encode()
		if err == nil {
			err = putRef(coll, refs, refName(i), root, raw)
		}
		if err != nil {
			closeAll(objects, refs, coll)
			return fmt.Errorf("ref %d: %w", i, err)
		}
		t2 := time.Now()
		res.Ingest = append(res.Ingest, refIngest{
			Index: i, Kept: m.Kept, Logical: m.Logical, Fresh: m.Fresh, Shared: m.Shared,
			Stored: ws.Stored, Deduped: ws.Deduped, BytesStored: ws.BytesStored,
			IngestNs: int64(t1.Sub(t0)), RefNs: int64(t2.Sub(t1)),
		})
		logical += m.Logical
		stored += ws.BytesStored
		if (i+1)%50 == 0 || i+1 == cfg.refs {
			el := time.Since(start)
			logf("ingest: %4d/%d  logical %s  stored %s  %.0f MiB/s logical  %.0f MiB/s stored",
				i+1, cfg.refs, human(logical), human(stored),
				float64(logical)/MiB/el.Seconds(), float64(stored)/MiB/el.Seconds())
		}
	}
	res.IngestWallNs = int64(time.Since(start))
	tc := time.Now()
	if err := closeAll(objects, refs, coll); err != nil {
		return err
	}
	res.IngestCloseNs = int64(time.Since(tc))
	logf("ingest: done in %s (+%s close)", time.Duration(res.IngestWallNs).Round(time.Millisecond),
		time.Duration(res.IngestCloseNs).Round(time.Millisecond))
	return saveResults(cfg, res)
}

// ---------------------------------------------------------------- delete

func phaseDelete(cfg *config, res *results) error {
	objects, refs, coll, err := openAll(cfg, gc.Options{})
	if err != nil {
		return err
	}
	logf("delete: removing the deleted-class refs")
	start := time.Now()
	n := 0
	for i := range cfg.refs {
		if kept(i) {
			continue
		}
		if err := rmRef(coll, refs, refName(i)); err != nil {
			closeAll(objects, refs, coll)
			return fmt.Errorf("rm ref %d: %w", i, err)
		}
		n++
	}
	res.DeleteNs = int64(time.Since(start))
	res.DeleteN = n
	if err := closeAll(objects, refs, coll); err != nil {
		return err
	}
	logf("delete: %d refs in %s", n, time.Duration(res.DeleteNs).Round(time.Millisecond))
	return saveResults(cfg, res)
}

// ---------------------------------------------------------------- gc

func runCLI(cfg *config, args ...string) cliRun {
	full := append([]string{"--store", cfg.store, "--segment-size", fmt.Sprint(cfg.segment)}, args...)
	t0 := time.Now()
	out, err := exec.Command(cfg.bin, full...).CombinedOutput()
	r := cliRun{Args: args, Output: string(out), WallNs: int64(time.Since(t0))}
	if err != nil {
		r.ExitErr = err.Error()
	}
	return r
}

func phaseGC(cfg *config, res *results) error {
	if cfg.bin == "" {
		return errors.New("gc: -bin (or amber-store in PATH) is required")
	}
	time.Sleep(2 * time.Second) // every sealed pack crosses a 1 s grace
	for _, args := range [][]string{
		{"gc", "run", "--grace", "1s"},
		{"gc", "run", "--grace", "1s", "--garbage", "0"},
	} {
		label := "after-gc-policy"
		if slices.Contains(args, "--garbage") {
			label = "after-gc-forced"
		}
		logf("gc: %s", strings.Join(args, " "))
		r := runCLI(cfg, args...)
		res.GCRuns = append(res.GCRuns, r)
		logf("gc: %s wall: %s%s", time.Duration(r.WallNs).Round(time.Millisecond), strings.TrimSpace(r.Output), r.ExitErr)
		if r.ExitErr != "" {
			saveResults(cfg, res)
			return fmt.Errorf("gc: %s: %s", r.ExitErr, r.Output)
		}
		if err := snapshotStore(cfg, res, label); err != nil {
			return err
		}
	}
	return nil
}

func snapshotStore(cfg *config, res *results, label string) error {
	s := snapshot{Label: label}
	for _, d := range []struct {
		sub string
		dst *int64
	}{{"packstore", &s.PackstoreKiB}, {"refs", &s.RefsKiB}, {"closures", &s.ClosuresKiB}} {
		n, err := duKiB(filepath.Join(cfg.store, d.sub))
		if err != nil {
			return err
		}
		*d.dst = n
	}
	sealed, _ := filepath.Glob(filepath.Join(cfg.store, "packstore", "*.seg"))
	active, _ := filepath.Glob(filepath.Join(cfg.store, "packstore", "*.seg.active"))
	s.Segments = len(sealed) + len(active)
	var st unix.Statfs_t
	if err := unix.Statfs(cfg.store, &st); err == nil {
		s.FreeBytes = uint64(st.Bavail) * uint64(st.Bsize)
	}
	if cfg.bin != "" {
		r := runCLI(cfg, "gc", "status")
		if r.ExitErr != "" {
			return fmt.Errorf("gc status: %s: %s", r.ExitErr, r.Output)
		}
		// The full per-pack listing goes next to the results file; the
		// totals go into it.
		listing := filepath.Join(filepath.Dir(cfg.out), "gc-status-"+label+".txt")
		if err := os.WriteFile(listing, []byte(r.Output), 0o644); err != nil {
			return err
		}
		var totals []string
		for _, line := range strings.Split(r.Output, "\n") {
			if strings.HasPrefix(line, "live ") || strings.HasPrefix(line, "last cycle") {
				totals = append(totals, line)
			}
		}
		s.GCStatus = strings.Join(totals, "\n")
	}
	res.Snapshots = append(res.Snapshots, s)
	logf("snapshot %s: packstore %s, refs %s, closures %s, %d segments (incl. active); %s",
		label, human(s.PackstoreKiB<<10), human(s.RefsKiB<<10), human(s.ClosuresKiB<<10), s.Segments,
		strings.ReplaceAll(s.GCStatus, "\n", " | "))
	return saveResults(cfg, res)
}

func duKiB(dir string) (int64, error) {
	out, err := exec.Command("du", "-sk", dir).Output()
	if err != nil {
		return 0, fmt.Errorf("du %s: %w", dir, err)
	}
	var n int64
	fmt.Sscan(string(out), &n)
	return n, nil
}

// ---------------------------------------------------------------- verify

func phaseVerify(cfg *config, res *results) error {
	objects, refs, coll, err := openAll(cfg, gc.Options{})
	if err != nil {
		return err
	}
	logf("verify: CheckComplete on every kept ref")
	res.VerifyComplete = 0
	res.VerifyErrors = nil
	for i := range cfg.refs {
		if !kept(i) {
			if _, err := refs.Get(refName(i)); !errors.Is(err, refstore.ErrNotFound) {
				res.VerifyErrors = append(res.VerifyErrors, fmt.Sprintf("ref %d still present: %v", i, err))
			}
			continue
		}
		root, err := rootOf(refs, refName(i))
		if err != nil {
			res.VerifyErrors = append(res.VerifyErrors, fmt.Sprintf("ref %d: %v", i, err))
			continue
		}
		if _, err := fstree.CheckComplete(root, objects.Get, objects.Has, 0); err != nil {
			res.VerifyErrors = append(res.VerifyErrors, fmt.Sprintf("ref %d incomplete: %v", i, err))
			continue
		}
		res.VerifyComplete++
	}
	if err := closeAll(objects, refs, coll); err != nil {
		return err
	}
	logf("verify: %d refs complete, %d errors", res.VerifyComplete, len(res.VerifyErrors))
	if cfg.bin != "" && cfg.restore != "" {
		// Byte-for-byte restore of a sample: kept refs whose predecessor was
		// deleted (i%10 == 0) share data with reaped packs — the risky case.
		res.VerifyRestoreOK = nil
		for i := 10; i < cfg.refs && i <= 100; i += 10 {
			dest := filepath.Join(cfg.restore, fmt.Sprintf("ref-%04d", i))
			os.RemoveAll(dest)
			r := runCLI(cfg, "restore", "ref:"+refName(i), dest)
			if r.ExitErr != "" {
				res.VerifyErrors = append(res.VerifyErrors, fmt.Sprintf("restore ref %d: %s: %s", i, r.ExitErr, r.Output))
				continue
			}
			out, err := exec.Command("diff", "-rq", refDir(cfg, i), dest).CombinedOutput()
			if err != nil {
				res.VerifyErrors = append(res.VerifyErrors, fmt.Sprintf("diff ref %d: %v: %s", i, err, out))
			} else {
				res.VerifyRestoreOK = append(res.VerifyRestoreOK, refName(i))
			}
			os.RemoveAll(dest)
		}
		logf("verify: %d sample restores identical, %d errors total", len(res.VerifyRestoreOK), len(res.VerifyErrors))
	}
	return saveResults(cfg, res)
}

// ---------------------------------------------------------------- report

func report(w io.Writer, r *results) error {
	if len(r.Ingest) == 0 || len(r.Manifests) == 0 {
		return errors.New("report: no ingest results yet")
	}
	p := func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
	var logical, fresh, shared, stored int64
	var nStored, nDeduped, files int
	var keptFresh, keptLogical, delFresh, delLogical int64
	for _, m := range r.Manifests {
		files += len(m.Files)
		if m.Kept {
			keptFresh, keptLogical = keptFresh+m.Fresh, keptLogical+m.Logical
		} else {
			delFresh, delLogical = delFresh+m.Fresh, delLogical+m.Logical
		}
	}
	var perKept, perDel, refPut []float64
	var ingestNs, refNs int64
	for _, x := range r.Ingest {
		logical, fresh, shared, stored = logical+x.Logical, fresh+x.Fresh, shared+x.Shared, stored+x.BytesStored
		nStored, nDeduped = nStored+x.Stored, nDeduped+x.Deduped
		ingestNs, refNs = ingestNs+x.IngestNs, refNs+x.RefNs
		ms := float64(x.IngestNs+x.RefNs) / 1e6
		if x.Kept {
			perKept = append(perKept, ms)
		} else {
			perDel = append(perDel, ms)
		}
		refPut = append(refPut, float64(x.RefNs)/1e6)
	}
	nKept, nDel := len(perKept), len(perDel)
	wall := float64(r.IngestWallNs) / 1e9
	p("DATASET  refs=%d files=%d logical=%s fresh=%s shared(copies)=%s overlap=%.1f%%",
		len(r.Manifests), files, human(logical), human(fresh), human(shared), 100*float64(shared)/float64(logical))
	p("         kept %d refs: fresh %s, logical %s", nKept, human(keptFresh), human(keptLogical))
	p("         deleted %d refs: fresh %s, logical %s", nDel, human(delFresh), human(delLogical))
	p("GEN      %.1fs", float64(r.GenNs)/1e9)
	p("INGEST   wall %.1fs  (ingest.Dir %.1fs + ref put/closure walk %.1fs; close %.2fs)",
		wall, float64(ingestNs)/1e9, float64(refNs)/1e9, float64(r.IngestCloseNs)/1e9)
	p("         logical %.0f MiB/s   new-bytes %.0f MiB/s   refs %.1f/s",
		float64(logical)/MiB/wall, float64(stored)/MiB/wall, float64(len(r.Ingest))/wall)
	p("         objects stored %d  deduped %d  bytes stored %s  (%.1f%% of ingested bytes deduplicated)",
		nStored, nDeduped, human(stored), 100*(1-float64(stored)/float64(logical)))
	if nKept > 0 && nDel > 0 {
		p("         per-ref ms: kept median %.0f p95 %.0f max %.0f | deleted-class median %.0f p95 %.0f max %.0f",
			median(perKept), pct(perKept, 0.95), slices.Max(perKept), median(perDel), pct(perDel, 0.95), slices.Max(perDel))
	}
	head, tail := refPut[:min(100, len(refPut))], refPut[max(0, len(refPut)-100):]
	p("         ref put (closure walk) ms: median %.1f p95 %.1f max %.1f; first %d median %.1f, last %d median %.1f",
		median(refPut), pct(refPut, 0.95), slices.Max(refPut), len(head), median(head), len(tail), median(tail))
	window := 100
	if len(r.Ingest) < 200 {
		window = max(1, len(r.Ingest)/5)
	}
	for w := 0; w < len(r.Ingest); w += window {
		xs := r.Ingest[w:min(w+window, len(r.Ingest))]
		var lg, ti, tr int64
		for _, x := range xs {
			lg, ti, tr = lg+x.Logical, ti+x.IngestNs, tr+x.RefNs
		}
		p("         refs %4d-%4d: %6.0f MiB/s  ingest.Dir %7.1f ms/ref  ref-put %6.1f ms/ref",
			w, w+len(xs)-1, float64(lg)/MiB/(float64(ti+tr)/1e9), float64(ti)/1e6/float64(len(xs)), float64(tr)/1e6/float64(len(xs)))
	}
	if r.DeleteN > 0 {
		p("DELETE   %d refs in %.2fs  (%.1f ms/ref)", r.DeleteN, float64(r.DeleteNs)/1e9, float64(r.DeleteNs)/1e6/float64(r.DeleteN))
	}
	for _, run := range r.GCRuns {
		p("GC RUN   %s: wall %.2fs -> %s %s", strings.Join(run.Args, " "), float64(run.WallNs)/1e9, strings.TrimSpace(run.Output), run.ExitErr)
	}
	if len(r.Snapshots) > 0 {
		p("SNAPSHOTS")
	}
	snap := map[string]snapshot{}
	for _, s := range r.Snapshots {
		snap[s.Label] = s
		p("  %-16s packstore %10s  segments %4d  closures %9s  refs-db %9s  | %s",
			s.Label, human(s.PackstoreKiB<<10), s.Segments, human(s.ClosuresKiB<<10), human(s.RefsKiB<<10),
			strings.ReplaceAll(s.GCStatus, "\n", " | "))
	}
	if a, ok := snap["after-ingest"]; ok {
		if b, ok := snap["after-gc-policy"]; ok {
			ab, bb := a.PackstoreKiB<<10, b.PackstoreKiB<<10
			p("RECLAIM  nominal (fresh bytes owned by the deleted refs): %s", human(delFresh))
			p("         policy GC freed on disk: %s (%.0f%% of nominal); store %s -> %s",
				human(ab-bb), 100*float64(ab-bb)/float64(delFresh), human(ab), human(bb))
			if c, ok := snap["after-gc-forced"]; ok {
				cb := c.PackstoreKiB << 10
				p("         forced  GC freed on disk (cumulative): %s (%.0f%% of nominal); store -> %s",
					human(ab-cb), 100*float64(ab-cb)/float64(delFresh), human(cb))
			}
		}
	}
	p("VERIFY   complete kept refs %d/%d; sample restores identical %d; errors %v",
		r.VerifyComplete, nKept, len(r.VerifyRestoreOK), r.VerifyErrors)
	return nil
}

func median(xs []float64) float64 { return pct(xs, 0.5) }

func pct(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*q))]
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
