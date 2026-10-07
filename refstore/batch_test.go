package refstore_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/draganm/rebma/refstore"
)

func TestPutBatchReopen(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			dir := t.TempDir()
			s, err := refstore.Open(dir, durable)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Put("unrelated", []byte("preserved")); err != nil {
				t.Fatal(err)
			}
			data := []byte("last")
			if err := s.PutBatch([]refstore.Record{{Name: "a", Data: []byte("first")}, {Name: "b", Data: []byte("second")}, {Name: "a", Data: data}, {Name: "", Data: nil}}); err != nil {
				t.Fatal(err)
			}
			data[0] = 'x'
			if err := s.PutBatch(nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = refstore.Open(dir, durable)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for name, want := range map[string][]byte{"a": []byte("last"), "b": []byte("second"), "unrelated": []byte("preserved"), "": nil} {
				got, err := s.Get(name)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%q = %q, %v", name, got, err)
				}
			}
		})
	}
}

func TestPutBatchAtomicSnapshots(t *testing.T) {
	s := open(t, t.TempDir())
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for generation := range 100 {
				value := []byte(fmt.Sprintf("%d/%d", worker, generation))
				if err := s.PutBatch([]refstore.Record{{Name: "a", Data: value}, {Name: "b", Data: value}}); err != nil {
					t.Error(err)
					return
				}
				all, err := s.All()
				if err != nil {
					t.Error(err)
					return
				}
				if len(all) != 2 || !bytes.Equal(all[0].Data, all[1].Data) {
					t.Error("partial batch became visible")
					return
				}
			}
		})
	}
	wg.Wait()
}
