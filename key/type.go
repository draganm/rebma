// Package key implements the 32-byte lookup key: a content address that
// encodes a CAS object type, a logical payload length, and a truncated BLAKE3
// hash of the payload's serialized bytes. It is the Amber-Store key with its
// bytes reversed, so the hash comes first and the header byte last. See
// architecture/keys.md.
package key

import "fmt"

// Type is the 4-bit CAS object type carried in the high nibble of a key's
// header byte, the key's last byte (architecture/types.md).
type Type uint8

const (
	Blob     Type = 0 // raw file-content byte chunk (a CDC leaf)
	FileNode Type = 1 // file chunk-index node
	DirLeaf  Type = 2 // a contiguous run of directory entries
	DirNode  Type = 3 // directory index node
	XattrSet Type = 4 // spilled extended attributes
	Commit   Type = 5 // snapshot record: tree, parent commits, author, committer, message
)

// IsValid reports whether t is a defined CAS object type (0..5). Types 6..15 are
// reserved and must not be emitted; values above 15 do not fit the 4-bit field.
func (t Type) IsValid() bool {
	return t <= Commit
}

// String returns the type name, or "Type(n)" for reserved/unknown values.
func (t Type) String() string {
	switch t {
	case Blob:
		return "Blob"
	case FileNode:
		return "FileNode"
	case DirLeaf:
		return "DirLeaf"
	case DirNode:
		return "DirNode"
	case XattrSet:
		return "XattrSet"
	case Commit:
		return "Commit"
	default:
		return fmt.Sprintf("Type(%d)", uint8(t))
	}
}
