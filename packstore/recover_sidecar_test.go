package packstore

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/rebma/amberpack"
)

// onlyActive returns the path of the directory's single active segment.
func onlyActive(t *testing.T, dir string) string {
	t.Helper()
	actives, err := filepath.Glob(filepath.Join(dir, "*"+activeSuffix))
	if err != nil || len(actives) != 1 {
		t.Fatalf("active segments = %v, %v; want exactly one", actives, err)
	}
	return actives[0]
}

func sidecars(t *testing.T, dir string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(dir, "*"+sidecarSuffix))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func wantObjects(t *testing.T, s *Store, objs []Object) {
	t.Helper()
	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(got, o.Data) {
			t.Fatalf("Get(%s): %d bytes, %v; want the %d bytes put", o.Key, len(got), err, len(o.Data))
		}
	}
}

// Entries covered by a synced record are trusted: opening does not read
// their records back. Today's full scan would have stopped at the damage and
// truncated; reporting damage in the body is scrub's job.
func TestOpenTrustsSyncedEntries(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 5)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data := onlyActive(t, dir)
	before, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	flipByte(t, data, int64(len(magicHeader))+amberpack.RecHeaderSize) // first payload byte of the first record

	s2 := openStore(t, dir)
	for _, o := range objs {
		if has, err := s2.Has(o.Key); err != nil || !has {
			t.Fatalf("Has(%s) = %v, %v after a reopen from the sidecar", o.Key, has, err)
		}
	}
	after, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("the data file went from %d to %d bytes: trusted records were re-read and truncated", before.Size(), after.Size())
	}
}

type countingReaderAt struct {
	r io.ReaderAt
	n int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.r.ReadAt(p, off)
	c.n += int64(n)
	return n, err
}

func TestRecoveryReadsOnlyTheUntrustedTail(t *testing.T) {
	objs := testObjects(t, 6)
	body, spans := buildBody(t, objs)
	entries := make([]sidecarRec, len(spans))
	for i, sp := range spans {
		rec, err := amberpack.ParseRecord(body[sp.off:])
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = entryRec(sp.obj.Key, activeLoc{off: sp.off, flags: rec.Flags, ulen: rec.Ulen, slen: rec.Slen})
	}
	probe := int64(len(magicHeader) + len(magicTrailer)) // the header check and the crashed-seal probe

	// Everything known durable: no record is read.
	all := sidecarImage(append(append([]sidecarRec{}, entries...), sidecarRec{kind: sidecarSynced, off: uint64(len(body))})...)
	cr := &countingReaderAt{r: bytes.NewReader(body)}
	res, ok, err := recoverFrom(cr, int64(len(body)), all)
	if err != nil || !ok {
		t.Fatalf("recoverFrom = ok %v, %v", ok, err)
	}
	if len(res.index) != len(objs) || res.dataEnd != int64(len(body)) || res.sidecarEnd != int64(len(all)) || len(res.missing) != 0 {
		t.Fatalf("recovered %d keys, dataEnd %d, sidecarEnd %d, missing %d", len(res.index), res.dataEnd, res.sidecarEnd, len(res.missing))
	}
	if cr.n > probe {
		t.Fatalf("recovery read %d data bytes with everything synced, want at most %d", cr.n, probe)
	}

	// Only the first two records known durable: exactly the rest is read.
	partial := sidecarImage(entries[0], entries[1], sidecarRec{kind: sidecarSynced, off: uint64(spans[2].off)}, entries[2], entries[3], entries[4], entries[5])
	cr = &countingReaderAt{r: bytes.NewReader(body)}
	res, ok, err = recoverFrom(cr, int64(len(body)), partial)
	if err != nil || !ok || len(res.index) != len(objs) {
		t.Fatalf("recoverFrom(partial) = %d keys, ok %v, %v", len(res.index), ok, err)
	}
	if want := probe + int64(len(body)) - spans[2].off; cr.n != want {
		t.Fatalf("recovery read %d data bytes, want %d: the unsynced records and nothing else", cr.n, want)
	}
}

// A writer killed after its data fsync returned but before the entry reached
// the sidecar leaves an acknowledged record the sidecar does not list.
func TestAcknowledgedRecordMissingFromIndexIsFound(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 3)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	idx := onlyActive(t, dir) + sidecarSuffix
	recs, valid := readSidecarFile(t, idx)
	// Put wrote entry+synced per object, Close one more synced: drop the
	// last object's entry and everything after it.
	if len(recs) != 7 {
		t.Fatalf("sidecar holds %d records, want 7", len(recs))
	}
	if err := os.Truncate(idx, int64(valid-3*sidecarRecSize)); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, dir)
	wantObjects(t, s2, objs) // found by a store that only reads, which repairs nothing
	// The next writer takes the segment and brings its sidecar in line.
	extra := testObjects(t, 4)[3:]
	putAll(t, s2, extra)
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	recs, _ = readSidecarFile(t, idx)
	listed := 0
	for _, r := range recs {
		if r.kind == sidecarEntry {
			listed++
		}
	}
	if listed != len(objs)+len(extra) {
		t.Fatalf("the sidecar lists %d records after recovery, want %d", listed, len(objs)+len(extra))
	}
}

// crashedCopy copies an open store's files, as a crash would leave them.
func crashedCopy(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	data := onlyActive(t, from)
	copyFile(t, data, filepath.Join(to, filepath.Base(data)))
	copyFile(t, data+sidecarSuffix, filepath.Join(to, filepath.Base(data)+sidecarSuffix))
	return to
}

func TestUnsyncedEntriesAreVerified(t *testing.T) {
	src := t.TempDir()
	s := openStore(t, src, WithSync(false))
	objs := testObjects(t, 5)
	if err := s.WriteBatch(objSeq(objs, -1)); err != nil {
		t.Fatal(err)
	}
	dir := crashedCopy(t, src) // no fsync ever ran: nothing is known durable
	data := onlyActive(t, dir)
	st, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	flipByte(t, data, st.Size()-1) // the last record's last payload byte

	s2 := openStore(t, dir, WithSync(false))
	wantObjects(t, s2, objs[:4])
	if has, _ := s2.Has(objs[4].Key); has {
		t.Fatal("a record that fails its CRC was indexed from its sidecar entry")
	}

	// Stale entries must never resolve into records appended later.
	more := testObjects(t, 9)[5:]
	for _, o := range more {
		if err := s2.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s3 := openStore(t, dir, WithSync(false))
		wantObjects(t, s3, objs[:4])
		wantObjects(t, s3, more)
		if has, _ := s3.Has(objs[4].Key); has {
			t.Fatal("the dropped record came back")
		}
		if err := s3.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSidecarProblemsFallBackToAFullScan(t *testing.T) {
	for name, damage := range map[string]func(t *testing.T, idx string, dataLen int64){
		"missing": func(t *testing.T, idx string, _ int64) {
			if err := os.Remove(idx); err != nil {
				t.Fatal(err)
			}
		},
		"garbage": func(t *testing.T, idx string, _ int64) {
			if err := os.WriteFile(idx, bytes.Repeat([]byte("junk"), 100), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"claims more durable data than the file holds": func(t *testing.T, idx string, dataLen int64) {
			if err := os.WriteFile(idx, sidecarImage(sidecarRec{kind: sidecarSynced, off: uint64(dataLen) + 1}), 0o644); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s := openStore(t, dir)
			objs := testObjects(t, 4)
			for _, o := range objs {
				if err := s.Put(o.Key, o.Data); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			data := onlyActive(t, dir)
			st, err := os.Stat(data)
			if err != nil {
				t.Fatal(err)
			}
			damage(t, data+sidecarSuffix, st.Size())

			s2 := openStore(t, dir)
			wantObjects(t, s2, objs)
			// The writer that takes the segment rebuilds its sidecar.
			extra := testObjects(t, 5)[4:]
			putAll(t, s2, extra)
			if err := s2.Close(); err != nil {
				t.Fatal(err)
			}
			recs, _ := readSidecarFile(t, data+sidecarSuffix)
			listed := 0
			for _, r := range recs {
				if r.kind == sidecarEntry {
					listed++
				}
			}
			if listed != len(objs)+len(extra) {
				t.Fatalf("the rebuilt sidecar lists %d records, want %d", listed, len(objs)+len(extra))
			}
		})
	}
}

func TestSealRemovesSidecar(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, WithSegmentSize(1)) // every record seals its segment
	for _, o := range testObjects(t, 3) {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if got := sidecars(t, dir); len(got) != 0 {
		t.Fatalf("sidecars left behind by sealed segments: %v", got)
	}
}

func TestCrashedSealLeavesNoSidecar(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 6)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The footer reached the file, the rename did not happen.
	data := onlyActive(t, dir)
	res, err := scanActive(data)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]indexEntry, 0, len(res.index))
	for k, loc := range res.index {
		entries = append(entries, indexEntry{k: k, off: uint64(loc.off), slen: loc.slen})
	}
	footer, err := buildFooter(res.size, entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(data, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2 := openStore(t, dir)
	wantObjects(t, s2, objs)
	putAll(t, s2, testObjects(t, 7)[6:]) // the write that takes the segment and finishes its seal
	if _, err := os.Stat(data + sidecarSuffix); !os.IsNotExist(err) {
		t.Fatalf("the completed seal left its sidecar: %v", err)
	}
	if got := sidecars(t, dir); len(got) != 1 {
		t.Fatalf("sidecars = %v, want only the new active segment's", got)
	}
}

func TestOrphanSidecarIsRemoved(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, "00000000000000aa"+activeSuffix+sidecarSuffix)
	if err := os.WriteFile(orphan, sidecarImage(), 0o644); err != nil {
		t.Fatal(err)
	}
	openStore(t, dir)
	if got := sidecars(t, dir); len(got) != 0 {
		t.Fatalf("an index without its segment survived the open: %v", got)
	}
}

func TestWipeRemovesSidecar(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	o := testObjects(t, 1)[0]
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if len(sidecars(t, dir)) != 1 {
		t.Fatal("a written active segment has no sidecar")
	}
	if err := s.Wipe(); err != nil {
		t.Fatal(err)
	}
	if got := sidecars(t, dir); len(got) != 0 {
		t.Fatalf("Wipe left %v", got)
	}
}
