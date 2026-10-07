package fstree

import (
	"fmt"
	"io"

	"github.com/draganm/rebma/key"
)

// WriteContent writes the regular-file content addressed by k to w, descending
// FileNode index levels and concatenating Blob leaves in order. k must be a
// Blob or FileNode object.
func WriteContent(w io.Writer, k key.Key, get func(key.Key) ([]byte, error)) error {
	data, err := get(k)
	if err != nil {
		return fmt.Errorf("reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.Blob:
		_, err := w.Write(data)
		return err
	case key.FileNode:
		children, err := DecodeFileNode(data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := WriteContent(w, ck, get); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s is not a file-content object (type %v)", k, k.Type())
	}
}
