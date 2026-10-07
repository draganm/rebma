package gc

import (
	"context"
	"testing"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/reference"
)

// Status reads the references, which every process sees at once, and marks
// from them. The objects they name may have been written by another process
// since this one last looked at the store: Status has to look first.
func TestStatusSeesWhatAnotherStoreWroteAndNamed(t *testing.T) {
	a := newTestStore(t, 1<<20)
	ca := a.openCollector(t, Options{})
	b := openTestStoreAt(t, a.dir, 1<<20)
	cb := b.openCollector(t, Options{})
	if _, err := cb.Status(context.Background()); err != nil { // b has looked at the store
		t.Fatal(err)
	}
	root, keys := storeTree(t, a.objects, "named elsewhere", 5)
	putTestRef(t, ca, a.refs, "main", root)
	st, err := cb.Status(context.Background())
	if err != nil {
		t.Fatalf("Status, after another store wrote a tree and named it: %v", err)
	}
	if st.Refs != 1 || st.Marked != len(keys) {
		t.Fatalf("Status = %d references, %d objects marked; want 1 and %d", st.Refs, st.Marked, len(keys))
	}
}

// A write inside a collector's write span must not wait for a wipe that is
// waiting for the span.
func TestWritesInsideACollectorSpanPassAWaitingWipe(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	done := c.BeginWrite()
	defer done() // lets everybody go when the test fails half way
	var wipeErr error
	wiped := async(func() { wipeErr = c.Wipe(ts.objects.Wipe) })
	stillWaiting(t, wiped, "a wipe, while a write span of its own process is open,")

	o, err := fstree.EncodeBlob([]byte("written inside the span"))
	if err != nil {
		t.Fatal(err)
	}
	var putErr error
	wrote := async(func() { putErr = ts.objects.Put(o.Key, o.Bytes) })
	finishes(t, wrote, "a write inside the span, with a wipe waiting for the span,")
	if putErr != nil {
		t.Fatal(putErr)
	}
	done()
	finishes(t, wiped, "the wipe, after the span ended,")
	if wipeErr != nil {
		t.Fatal(wipeErr)
	}
}

// A span opened through the collector takes the reference lock before the
// store's gate, the order a cycle takes them in, and prepares references
// without taking either again. So a reference can be put inside it while a
// cycle of the same process waits for it to end.
func TestSpanPutsAReferenceWhileACycleWaits(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	sp, err := c.BeginSpan()
	if err != nil {
		t.Fatal(err)
	}
	defer sp.End() // lets everybody go when the test fails half way
	root, keys := storeTree(t, ts.objects, "inside a span", 5)

	var runErr error
	ran := async(func() { _, runErr = c.Run(context.Background(), 0) })
	stillWaiting(t, ran, "a cycle, while a span of its own process is open,")

	var putErr error
	put := async(func() {
		commit, _, err := sp.PrepareRef(root)
		if err == nil {
			var raw []byte
			rec := reference.Reference{Name: "main", Key: root[:], CreatedAt: time.Now().UnixNano()}
			if raw, err = rec.Encode(); err == nil {
				err = ts.refs.Put("main", raw)
			}
			commit()
		}
		putErr = err
	})
	finishes(t, put, "a reference put inside the span, with a cycle waiting for the span,")
	if putErr != nil {
		t.Fatal(putErr)
	}
	sp.End()
	finishes(t, ran, "the cycle, after the span ended,")
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, k := range keys {
		if has, err := ts.objects.Has(k); err != nil || !has {
			t.Fatalf("Has(%s) = %v, %v after the cycle: the reference names it", k, has, err)
		}
	}
	if _, _, err := sp.PrepareRef(root); err == nil {
		t.Fatal("PrepareRef on an ended span took no lock and reported no error")
	}
}
