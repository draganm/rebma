package fstree_test

import (
	"reflect"
	"testing"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

func TestListEntries_PagesMatchCollect(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	want, err := fstree.CollectEntries(root, store.get)
	if err != nil {
		t.Fatal(err)
	}

	for _, limit := range []int{1, 7, 100, 999, 1000, 5000} {
		var got []fstree.Entry
		var after []byte
		for {
			page, more, err := fstree.ListEntries(root, after, limit, store.get)
			if err != nil {
				t.Fatalf("limit %d: %v", limit, err)
			}
			if len(page) > limit {
				t.Fatalf("limit %d: page has %d entries", limit, len(page))
			}
			got = append(got, page...)
			if !more {
				break
			}
			if len(page) == 0 {
				t.Fatalf("limit %d: more=true with an empty page", limit)
			}
			after = page[len(page)-1].Name
		}
		if len(got) != len(want) {
			t.Fatalf("limit %d: got %d entries, want %d", limit, len(got), len(want))
		}
		for i := range want {
			if !reflect.DeepEqual(got[i], want[i]) {
				t.Fatalf("limit %d: entry %d = %+v, want %+v", limit, i, got[i], want[i])
			}
		}
	}
}

func TestListEntries_MoreFlag(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)

	// limit == remaining entries: full page, nothing more.
	page, more, err := fstree.ListEntries(root, nil, 1000, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1000 || more {
		t.Fatalf("limit=1000: len=%d more=%v, want 1000 false", len(page), more)
	}

	// limit one short: more must be true.
	page, more, err = fstree.ListEntries(root, nil, 999, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 999 || !more {
		t.Fatalf("limit=999: len=%d more=%v, want 999 true", len(page), more)
	}

	// cursor in the middle of a leaf run starts strictly after it.
	page, _, err = fstree.ListEntries(root, []byte("e00007"), 3, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || string(page[0].Name) != "e00008" {
		t.Fatalf("after e00007: first = %q (len %d), want e00008", page[0].Name, len(page))
	}

	// cursor past the last entry: empty page, no more.
	page, more, err = fstree.ListEntries(root, []byte("e00999"), 10, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 || more {
		t.Fatalf("after last: len=%d more=%v, want 0 false", len(page), more)
	}
}

func TestListEntries_EmptyDir(t *testing.T) {
	store := memStore{}
	leaf, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(leaf); err != nil {
		t.Fatal(err)
	}
	page, more, err := fstree.ListEntries(leaf.Key, nil, 10, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 || more {
		t.Fatalf("empty dir: len=%d more=%v, want 0 false", len(page), more)
	}
}

func TestListEntries_TouchesFewObjects(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	reads := 0
	counting := func(k key.Key) ([]byte, error) {
		reads++
		return store.get(k)
	}
	if _, _, err := fstree.ListEntries(root, []byte("e00500"), 10, counting); err != nil {
		t.Fatal(err)
	}
	// A 10-entry page from the middle needs the root path plus a few
	// leaves; reading dozens of objects means subtree skipping is broken.
	if reads > 12 {
		t.Fatalf("page read %d objects, want a bounded walk", reads)
	}
}

func TestListEntries_BadLimit(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	for _, limit := range []int{0, -5} {
		if _, _, err := fstree.ListEntries(root, nil, limit, store.get); err == nil {
			t.Fatalf("limit %d: expected an error", limit)
		}
	}
}

func TestListEntries_RejectsNonDir(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fstree.ListEntries(blob.Key, nil, 10, store.get); err == nil {
		t.Fatal("expected error for a non-directory key")
	}
}
