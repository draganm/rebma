package fstree_test

import (
	"errors"
	"testing"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// commitObj builds a commit of tree with the given parents.
func commitObj(t *testing.T, msg string, tree key.Key, parents ...key.Key) fstree.Object {
	t.Helper()
	id := commit.Identity{Name: "Ann", Email: "ann@example.com", When: 1, TZOffset: 60}
	c := commit.Commit{Tree: tree, Parents: parents, Author: id, Committer: id, Message: msg}
	k, b, err := c.Object()
	if err != nil {
		t.Fatal(err)
	}
	return fstree.Object{Key: k, Bytes: b}
}

// history builds two trees and a diamond of commits over them:
//
//	first(treeA) <- left(treeB), right(treeA) <- tip(treeB), a merge.
//
// It returns every object, and the named ones.
func history(t *testing.T) (all []fstree.Object, first, tip fstree.Object) {
	t.Helper()
	treeA := completeTree(t)
	rootA := treeA[len(treeA)-1]
	blobC, err := fstree.EncodeBlob([]byte("gamma"))
	if err != nil {
		t.Fatal(err)
	}
	rootB, err := fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("c.txt"), Mode: 0o100644, ContentKey: blobC.Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	first = commitObj(t, "first", rootA.Key)
	left := commitObj(t, "left", rootB.Key, first.Key)
	right := commitObj(t, "right", rootA.Key, first.Key)
	tip = commitObj(t, "merge", rootB.Key, left.Key, right.Key)
	all = append(append([]fstree.Object{}, treeA...), blobC, rootB, first, left, right, tip)
	return all, first, tip
}

func TestChildKeysCommit(t *testing.T) {
	tree, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	p1 := commitObj(t, "p1", tree.Key)
	p2 := commitObj(t, "p2", tree.Key)
	c := commitObj(t, "merge", tree.Key, p2.Key, p1.Key)
	kids, err := fstree.ChildKeys(c.Key, c.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	want := []key.Key{tree.Key, p2.Key, p1.Key} // the tree, then parents in recorded order
	if len(kids) != len(want) {
		t.Fatalf("children = %v, want %v", kids, want)
	}
	for i := range want {
		if kids[i] != want[i] {
			t.Errorf("child %d = %s, want %s", i, kids[i], want[i])
		}
	}
	if _, err := fstree.ChildKeys(c.Key, []byte("not a commit")); err == nil {
		t.Error("ChildKeys accepted garbage under a Commit key")
	}
}

func TestReachableKeysFollowsHistory(t *testing.T) {
	all, _, tip := history(t)
	keys, err := fstree.ReachableKeys(tip.Key, mapGetter(all...))
	if err != nil {
		t.Fatal(err)
	}
	if keys[0] != tip.Key {
		t.Errorf("keys[0] = %s, want the tip", keys[0])
	}
	got := map[key.Key]int{}
	for _, k := range keys {
		got[k]++
	}
	for _, o := range all {
		if got[o.Key] != 1 {
			t.Errorf("%s (%v) listed %d times, want once", o.Key, o.Key.Type(), got[o.Key])
		}
	}
	if len(keys) != len(all) {
		t.Errorf("%d keys reachable, want %d", len(keys), len(all))
	}
}

func TestCheckCompleteFollowsHistory(t *testing.T) {
	all, first, tip := history(t)
	visited, err := fstree.CheckComplete(tip.Key, mapGetter(all...), mapHas(all...), 4)
	if err != nil {
		t.Fatalf("CheckComplete: %v", err)
	}
	if len(visited) != len(all) {
		t.Errorf("visited %d keys, want %d", len(visited), len(all))
	}

	without := func(drop key.Key) []fstree.Object {
		var out []fstree.Object
		for _, o := range all {
			if o.Key != drop {
				out = append(out, o)
			}
		}
		return out
	}
	// An ancestor commit is gone: the history is incomplete.
	rest := without(first.Key)
	if _, err := fstree.CheckComplete(tip.Key, mapGetter(rest...), mapHas(rest...), 4); err == nil {
		t.Error("CheckComplete accepted a history with a missing ancestor")
	}
	// A blob only the first commit's tree holds is gone: also incomplete.
	blobB := all[1]
	rest = without(blobB.Key)
	_, err = fstree.CheckComplete(tip.Key, mapGetter(rest...), mapHas(rest...), 4)
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != blobB.Key {
		t.Errorf("err = %v, want MissingObjectError for %s", err, blobB.Key)
	}
}

// oneFileTree is a directory holding one regular file, and that file's blob.
func oneFileTree(t *testing.T, name, content string) (tree, blob fstree.Object) {
	t.Helper()
	blob, err := fstree.EncodeBlob([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	tree, err = fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte(name), Mode: 0o100644, ContentKey: blob.Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	return tree, blob
}

// conflictedObj builds a commit of the conflicted tree: tree, then terms.
func conflictedObj(t *testing.T, tree key.Key, terms []key.Key, parents ...key.Key) fstree.Object {
	t.Helper()
	id := commit.Identity{Name: "Ann", Email: "ann@example.com", When: 1, TZOffset: 60}
	c := commit.Commit{Tree: tree, ConflictTerms: terms, Parents: parents, Author: id, Committer: id, Message: "conflict"}
	k, b, err := c.Object()
	if err != nil {
		t.Fatal(err)
	}
	return fstree.Object{Key: k, Bytes: b}
}

func without(all []fstree.Object, drop key.Key) []fstree.Object {
	var out []fstree.Object
	for _, o := range all {
		if o.Key != drop {
			out = append(out, o)
		}
	}
	return out
}

func TestChildKeysConflictedCommit(t *testing.T) {
	a, _ := oneFileTree(t, "f", "ours")
	r, _ := oneFileTree(t, "f", "base")
	b, _ := oneFileTree(t, "f", "theirs")
	p := commitObj(t, "p", a.Key)
	c := conflictedObj(t, a.Key, []key.Key{r.Key, b.Key}, p.Key)
	kids, err := fstree.ChildKeys(c.Key, c.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	want := []key.Key{a.Key, r.Key, b.Key, p.Key} // the tree, the conflict's other terms, then the parents
	if len(kids) != len(want) {
		t.Fatalf("children = %v, want %v", kids, want)
	}
	for i := range want {
		if kids[i] != want[i] {
			t.Errorf("child %d = %s, want %s", i, kids[i], want[i])
		}
	}
}

// Every side of a conflict is transferred, required and kept alive with the
// commit that records it.
func TestReachabilityCoversEveryConflictTerm(t *testing.T) {
	a, blobA := oneFileTree(t, "f", "ours")
	r, blobR := oneFileTree(t, "f", "base")
	b, blobB := oneFileTree(t, "f", "theirs")
	c := conflictedObj(t, a.Key, []key.Key{r.Key, b.Key})
	all := []fstree.Object{a, blobA, r, blobR, b, blobB, c}

	keys, err := fstree.ReachableKeys(c.Key, mapGetter(all...))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(all) {
		t.Fatalf("%d keys reachable, want all %d", len(keys), len(all))
	}
	if _, err := fstree.CheckComplete(c.Key, mapGetter(all...), mapHas(all...), 4); err != nil {
		t.Fatalf("CheckComplete: %v", err)
	}
	rest := without(all, blobB.Key)
	_, err = fstree.CheckComplete(c.Key, mapGetter(rest...), mapHas(rest...), 4)
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != blobB.Key {
		t.Fatalf("err = %v, want MissingObjectError for the last term's blob %s", err, blobB.Key)
	}
}

// vendored is a tree whose "vendor" entry is a directory entry holding a
// commit: top/{main.go, vendor -> commit(inner/{README, sub/{file}})}, the
// commit having one parent.
type vendored struct {
	all                                            []fstree.Object
	top, inner, sub, blobFile, base, commit, blobM fstree.Object
}

func vendoredTree(t *testing.T) vendored {
	t.Helper()
	var v vendored
	var blobR fstree.Object
	v.sub, v.blobFile = oneFileTree(t, "file", "inner file")
	blobR, err := fstree.EncodeBlob([]byte("readme"))
	if err != nil {
		t.Fatal(err)
	}
	v.inner, err = fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("README"), Mode: 0o100644, ContentKey: blobR.Key[:]},
		{Name: []byte("sub"), Mode: 0o040755, ContentKey: v.sub.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	v.base = commitObj(t, "base", v.inner.Key)
	v.commit = commitObj(t, "vendored", v.inner.Key, v.base.Key)
	v.blobM, err = fstree.EncodeBlob([]byte("package main"))
	if err != nil {
		t.Fatal(err)
	}
	v.top, err = fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("main.go"), Mode: 0o100644, ContentKey: v.blobM.Key[:]},
		{Name: []byte("vendor"), Mode: 0o040755, ContentKey: v.commit.Key[:]}, // a directory entry, a commit's key
	})
	if err != nil {
		t.Fatal(err)
	}
	v.all = []fstree.Object{v.sub, v.blobFile, blobR, v.inner, v.base, v.commit, v.blobM, v.top}
	return v
}

func TestDirOf(t *testing.T) {
	v := vendoredTree(t)
	node := completeTree(t)
	nodeRoot := node[len(node)-1]
	a, _ := oneFileTree(t, "f", "ours")
	r, _ := oneFileTree(t, "f", "base")
	conflict := conflictedObj(t, a.Key, []key.Key{r.Key, v.inner.Key})
	get := mapGetter(append(append(v.all, node...), a, r, conflict)...)
	for name, tc := range map[string]struct{ in, want key.Key }{
		"a leaf":              {v.inner.Key, v.inner.Key},
		"a node":              {nodeRoot.Key, nodeRoot.Key},
		"a commit":            {v.commit.Key, v.inner.Key},
		"a conflicted commit": {conflict.Key, a.Key}, // its first side
	} {
		got, err := fstree.DirOf(tc.in, get)
		if err != nil || got != tc.want {
			t.Errorf("%s: DirOf = %s, %v; want %s", name, got, err, tc.want)
		}
	}
	if _, err := fstree.DirOf(v.blobM.Key, get); err == nil {
		t.Error("DirOf accepted a blob")
	}
	if _, err := fstree.DirOf(v.commit.Key, mapGetter(without(v.all, v.commit.Key)...)); err == nil {
		t.Error("DirOf accepted a commit that is not there")
	}
}

// A commit key reads as the directory it records, wherever a directory key
// can stand: as the root of a walk and as the content key of a directory
// entry, in the middle of a path and at its end.
func TestReadersPassThroughACommit(t *testing.T) {
	v := vendoredTree(t)
	get := mapGetter(v.all...)

	if e, err := fstree.LookupEntry(v.commit.Key, []byte("README"), get); err != nil || string(e.Name) != "README" {
		t.Errorf("LookupEntry(commit) = %q, %v", e.Name, err)
	}
	if page, more, err := fstree.ListEntries(v.commit.Key, nil, 10, get); err != nil || more || len(page) != 2 || string(page[1].Name) != "sub" {
		t.Errorf("ListEntries(commit) = %d entries, more %v, %v", len(page), more, err)
	}
	if all, err := fstree.CollectEntries(v.commit.Key, get); err != nil || len(all) != 2 {
		t.Errorf("CollectEntries(commit) = %d entries, %v", len(all), err)
	}
	if k, err := fstree.ResolvePath(v.top.Key, "vendor/sub", get); err != nil || k != v.sub.Key {
		t.Errorf("ResolvePath through the entry = %s, %v; want %s", k, err, v.sub.Key)
	}
	e, err := fstree.ResolveEntry(v.top.Key, "vendor/sub/file", get)
	if err != nil || e == nil || string(e.Name) != "file" || string(e.ContentKey) != string(v.blobFile.Key[:]) {
		t.Errorf("ResolveEntry through the entry = %+v, %v", e, err)
	}
	// The entry's key is returned as stored: the caller can tell a commit.
	if k, err := fstree.ResolvePath(v.top.Key, "vendor", get); err != nil || k != v.commit.Key {
		t.Errorf("ResolvePath to the entry = %s, %v; want the commit key %s", k, err, v.commit.Key)
	}
}

// A directory's length counts a commit entry like any other: by its key's
// length field, which is the snapshot's footprint.
func TestDirLeafLengthCountsACommitEntry(t *testing.T) {
	v := vendoredTree(t)
	if want := uint64(len(v.commit.Bytes)) + v.inner.Key.Length(); v.commit.Key.Length() != want {
		t.Fatalf("commit key length = %d, want own bytes plus its tree, %d", v.commit.Key.Length(), want)
	}
	want := uint64(len(v.top.Bytes)) + v.blobM.Key.Length() + v.commit.Key.Length()
	if v.top.Key.Length() != want {
		t.Fatalf("directory length = %d, want %d", v.top.Key.Length(), want)
	}
}

func TestCheckCompleteThroughACommitEntry(t *testing.T) {
	v := vendoredTree(t)
	visited, err := fstree.CheckComplete(v.top.Key, mapGetter(v.all...), mapHas(v.all...), 4)
	if err != nil || len(visited) != len(v.all) {
		t.Fatalf("CheckComplete = %d keys, %v; want all %d: the commit, its tree and its history", len(visited), err, len(v.all))
	}
	for name, drop := range map[string]key.Key{"the commit": v.commit.Key, "its tree": v.inner.Key, "its parent": v.base.Key} {
		rest := without(v.all, drop)
		if _, err := fstree.CheckComplete(v.top.Key, mapGetter(rest...), mapHas(rest...), 4); err == nil {
			t.Errorf("CheckComplete accepted a tree that holds a commit without %s", name)
		}
	}
	rest := without(v.all, v.blobFile.Key)
	_, err = fstree.CheckComplete(v.top.Key, mapGetter(rest...), mapHas(rest...), 4)
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != v.blobFile.Key {
		t.Errorf("err = %v, want MissingObjectError for the file beneath the commit, %s", err, v.blobFile.Key)
	}
}
