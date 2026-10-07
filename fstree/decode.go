package fstree

import (
	"fmt"

	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
)

// DecodeFileNode decodes a FileNode body (a CBOR array of child keys) into its
// child keys, in file order. The inverse of EncodeFileNode.
func DecodeFileNode(b []byte) ([]key.Key, error) {
	var raw [][]byte
	if err := cbor.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("fstree: decoding file node: %w", err)
	}
	keys := make([]key.Key, len(raw))
	for i, r := range raw {
		k, err := key.Parse(r)
		if err != nil {
			return nil, fmt.Errorf("fstree: file node child %d: %w", i, err)
		}
		keys[i] = k
	}
	return keys, nil
}

// DecodeDirLeaf decodes a DirLeaf body (a CBOR array of entry maps) into its
// entries, in name order. The inverse of EncodeDirLeaf.
func DecodeDirLeaf(b []byte) ([]Entry, error) {
	var entries []Entry
	if err := cbor.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("fstree: decoding dir leaf: %w", err)
	}
	return entries, nil
}

// DecodeDirNode decodes a DirNode body (a CBOR array of [sepName, childKey]
// pairs) into its pairs, in sepName order. The inverse of EncodeDirNode.
func DecodeDirNode(b []byte) ([]DirPair, error) {
	var pairs []DirPair
	if err := cbor.Unmarshal(b, &pairs); err != nil {
		return nil, fmt.Errorf("fstree: decoding dir node: %w", err)
	}
	return pairs, nil
}
