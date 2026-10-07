package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
	"golang.org/x/sys/unix"
)

// resolveSpec parses a content spec: either KEY[/PATH] (lowercase-hex key,
// slash-separated subpath) or ref:NAME[@PATH] (reference name, '@'-separated
// subpath — '@' is banned in names, so the first '@' is unambiguous).
// Reference names resolve through the store's references DB.
func resolveSpec(refs *refstore.Store, s string) (key.Key, string, error) {
	rest, isRef := strings.CutPrefix(s, "ref:")
	if !isRef {
		return parseKeyPath(s)
	}
	name, path, _ := strings.Cut(rest, "@")
	if err := reference.ValidateName(name); err != nil {
		return key.Key{}, "", fmt.Errorf("invalid reference spec %q: %w", s, err)
	}
	raw, err := refs.Get(name)
	if err != nil {
		return key.Key{}, "", err
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		return key.Key{}, "", fmt.Errorf("reference %q: %w", name, err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, "", fmt.Errorf("reference %q: stored key: %w", name, err)
	}
	return k, path, nil
}

// parseKeyPath splits a KEY[/PATH] argument at the first slash and decodes the
// key part. The returned path is empty when no slash follows the key.
func parseKeyPath(s string) (key.Key, string, error) {
	keyPart, path, _ := strings.Cut(s, "/")
	k, err := parseHexKey(keyPart)
	if err != nil {
		return key.Key{}, "", err
	}
	return k, path, nil
}

// parseHexKey decodes a lowercase-hex key argument into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	k, err := key.Parse(raw)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return k, nil
}

// descend resolves a slash-separated subpath from root and returns the target
// entry's content key, as stored. Every traversed segment must be an entry
// carrying a content key (a regular file or a directory). A Commit, as the
// root or as the content key of a directory entry on the way, stands for its
// tree: fstree's readers pass through it. The key returned may itself be a
// commit's; every reader takes it, and fstree.DirOf names its directory.
func descend(objects *packstore.Store, root key.Key, path string) (key.Key, error) {
	k := root
	for seg := range strings.SplitSeq(path, "/") {
		if seg == "" {
			continue
		}
		e, err := fstree.LookupEntry(k, []byte(seg), objects.Get)
		if err != nil {
			return key.Key{}, fmt.Errorf("resolving %q: %w", path, err)
		}
		ck, err := key.Parse(e.ContentKey)
		if err != nil {
			return key.Key{}, fmt.Errorf("resolving %q: %q is not a file or directory", path, seg)
		}
		// The codec does not hold an entry's content key to its mode. A
		// commit under anything but a directory entry is a malformed tree.
		if ck.Type() == key.Commit && e.Mode&unix.S_IFMT != unix.S_IFDIR {
			return key.Key{}, fmt.Errorf("resolving %q: %q holds a commit but is not a directory entry", path, seg)
		}
		k = ck
	}
	return k, nil
}
