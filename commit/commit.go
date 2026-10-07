// Package commit defines the Commit object (CAS type 5): the analogue of a git
// commit — a directory root, ordered parent commits, author, committer and
// message, with an optional opaque signature — extended with what a jj commit
// carries besides: a change id, and the further terms and the labels of a
// conflicted tree. Encoding is RFC 8949 §4.2 core-deterministic CBOR
// (canonical map, integer keys), the fstree and reference convention. The
// key's length field is a footprint, like a directory's: the commit's own
// bytes plus its trees. See architecture/commits.md.
package commit

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
)

const (
	// MaxParents is the maximum number of parent commits.
	MaxParents = 256
	// MaxIdentityLen is the maximum byte length of an identity's name or email.
	MaxIdentityLen = 1024
	// MaxMessageLen is the maximum message length in bytes (1 MiB).
	MaxMessageLen = 1 << 20
	// MaxSignatureLen is the maximum Signature length in bytes (64 KiB).
	MaxSignatureLen = 64 << 10
	// MaxPublicKeyLen is the maximum PublicKey length in bytes (16 KiB).
	MaxPublicKeyLen = 16 << 10
	// MaxTZOffset bounds Identity.TZOffset to under a day either side of UTC.
	MaxTZOffset = 1439
	// MaxChangeIDLen is the maximum ChangeID length in bytes.
	MaxChangeIDLen = 64
	// MaxConflictTerms is the maximum number of conflict terms after the
	// tree. The number is always even: a remove for every further add.
	MaxConflictTerms = 254
	// MaxLabelLen is the maximum byte length of one conflict label. jj makes
	// a label from a commit's short ids and the whole first line of its
	// description, which nothing bounds; its adapter truncates beyond this.
	MaxLabelLen = 64 << 10
)

// encMode is the shared deterministic encoder, mirroring fstree.encMode.
// NilContainerAsEmpty makes a root commit's nil Parents the empty array.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("commit: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Identity is who acted and when: git's "Name <email> time tz".
type Identity struct {
	Name     string // 0..MaxIdentityLen bytes, no control characters; empty when the user configured none
	Email    string // 0..MaxIdentityLen bytes, no control characters
	When     int64  // ns since the Unix epoch
	TZOffset int    // minutes east of UTC, -MaxTZOffset..MaxTZOffset
}

// Commit is a snapshot record. Parents are ordered — the first is the
// mainline — and empty for a root commit. Signature and PublicKey are carried
// opaquely; the core neither creates nor verifies signatures.
//
// A conflicted commit records the tree A0 − R0 + A1 − R1 + …: Tree holds A0
// and ConflictTerms holds R0, A1, R1, A2, … (jj's order), so their number is
// even. Wherever a commit stands for a directory it stands for Tree.
type Commit struct {
	Tree      key.Key   // DirLeaf or DirNode
	Parents   []key.Key // Commit keys, no duplicates
	Author    Identity
	Committer Identity
	Message   string // UTF-8, may be empty
	Signature []byte // raw SSHSIG blob, nil when unsigned
	PublicKey []byte // signer's key, SSH wire format, nil when absent

	ChangeID       []byte    // opaque, 1..MaxChangeIDLen bytes: follows the change through rewrites; nil when absent
	ConflictTerms  []key.Key // DirLeaf or DirNode keys, an even number; nil when the tree is resolved
	ConflictLabels []string  // one per term counting Tree, at least one non-empty; nil when no term is labelled
}

// Trees returns every tree the commit records: Tree, then the conflict terms
// in order. The key's length and the object graph both go by this list.
func (c Commit) Trees() []key.Key {
	return append([]key.Key{c.Tree}, c.ConflictTerms...)
}

// Conflicted reports whether the commit records a conflicted tree.
func (c Commit) Conflicted() bool { return len(c.ConflictTerms) > 0 }

// wireIdentity and wireCommit are the encoded shapes: canonical CBOR maps with
// integer keys, fields declared in ascending key order. Every identity key and
// commit keys 0-4 are always present; 5-9 are omitted when absent.
type wireIdentity struct {
	Name     string `cbor:"0,keyasint"`
	Email    string `cbor:"1,keyasint"`
	When     int64  `cbor:"2,keyasint"`
	TZOffset int64  `cbor:"3,keyasint"`
}

type wireCommit struct {
	Tree      []byte       `cbor:"0,keyasint"`
	Parents   [][]byte     `cbor:"1,keyasint"`
	Author    wireIdentity `cbor:"2,keyasint"`
	Committer wireIdentity `cbor:"3,keyasint"`
	Message   string       `cbor:"4,keyasint"`
	Signature []byte       `cbor:"5,keyasint,omitempty"`
	PublicKey []byte       `cbor:"6,keyasint,omitempty"`

	ChangeID       []byte   `cbor:"7,keyasint,omitempty"`
	ConflictTerms  [][]byte `cbor:"8,keyasint,omitempty"`
	ConflictLabels []string `cbor:"9,keyasint,omitempty"`
}

func (id Identity) wire() wireIdentity {
	return wireIdentity{Name: id.Name, Email: id.Email, When: id.When, TZOffset: int64(id.TZOffset)}
}

func (c Commit) wire() wireCommit {
	parents := make([][]byte, len(c.Parents))
	for i := range c.Parents {
		parents[i] = c.Parents[i][:]
	}
	var terms [][]byte
	if len(c.ConflictTerms) > 0 {
		terms = make([][]byte, len(c.ConflictTerms))
		for i := range c.ConflictTerms {
			terms[i] = c.ConflictTerms[i][:]
		}
	}
	return wireCommit{
		Tree:           c.Tree[:],
		Parents:        parents,
		Author:         c.Author.wire(),
		Committer:      c.Committer.wire(),
		Message:        c.Message,
		Signature:      c.Signature,
		PublicKey:      c.PublicKey,
		ChangeID:       c.ChangeID,
		ConflictTerms:  terms,
		ConflictLabels: c.ConflictLabels,
	}
}

func (w wireIdentity) identity() (Identity, error) {
	if w.TZOffset < -MaxTZOffset || w.TZOffset > MaxTZOffset {
		return Identity{}, fmt.Errorf("tz offset %d outside ±%d minutes", w.TZOffset, MaxTZOffset)
	}
	return Identity{Name: w.Name, Email: w.Email, When: w.When, TZOffset: int(w.TZOffset)}, nil
}

func (w wireCommit) commit() (Commit, error) {
	tree, err := key.Parse(w.Tree)
	if err != nil {
		return Commit{}, fmt.Errorf("tree: %w", err)
	}
	var parents []key.Key
	if len(w.Parents) > 0 {
		parents = make([]key.Key, len(w.Parents))
	}
	for i, raw := range w.Parents {
		if parents[i], err = key.Parse(raw); err != nil {
			return Commit{}, fmt.Errorf("parent %d: %w", i, err)
		}
	}
	author, err := w.Author.identity()
	if err != nil {
		return Commit{}, fmt.Errorf("author: %w", err)
	}
	committer, err := w.Committer.identity()
	if err != nil {
		return Commit{}, fmt.Errorf("committer: %w", err)
	}
	// An array or byte string that is present but empty decodes to an empty,
	// non-nil value, re-encodes to nothing, and so fails Decode's canonical
	// check: absence has one encoding.
	var terms []key.Key
	if len(w.ConflictTerms) > 0 {
		terms = make([]key.Key, len(w.ConflictTerms))
	}
	for i, raw := range w.ConflictTerms {
		if terms[i], err = key.Parse(raw); err != nil {
			return Commit{}, fmt.Errorf("conflict term %d: %w", i, err)
		}
	}
	return Commit{
		Tree:           tree,
		Parents:        parents,
		Author:         author,
		Committer:      committer,
		Message:        w.Message,
		Signature:      w.Signature,
		PublicKey:      w.PublicKey,
		ChangeID:       w.ChangeID,
		ConflictTerms:  terms,
		ConflictLabels: w.ConflictLabels,
	}, nil
}

// validateText checks an identity string or a conflict label: at most max
// bytes of valid UTF-8 with no control characters.
func validateText(s string, max int) error {
	if len(s) > max {
		return fmt.Errorf("exceeds %d bytes", max)
	}
	if !utf8.ValidString(s) {
		return errors.New("must be valid UTF-8")
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return errors.New("must not contain control characters")
		}
	}
	return nil
}

func (id Identity) validate() error {
	// An empty name is what a user who configured none commits under (jj).
	if err := validateText(id.Name, MaxIdentityLen); err != nil {
		return fmt.Errorf("name %w", err)
	}
	if err := validateText(id.Email, MaxIdentityLen); err != nil {
		return fmt.Errorf("email %w", err)
	}
	if id.TZOffset < -MaxTZOffset || id.TZOffset > MaxTZOffset {
		return fmt.Errorf("tz offset %d outside ±%d minutes", id.TZOffset, MaxTZOffset)
	}
	return nil
}

// validate checks the whole record against the rules in architecture/commits.md.
func (c Commit) validate() error {
	if err := c.Tree.Validate(); err != nil {
		return fmt.Errorf("commit tree: %w", err)
	}
	if t := c.Tree.Type(); t != key.DirLeaf && t != key.DirNode {
		return fmt.Errorf("commit tree %s is not a directory key (type %v)", c.Tree, t)
	}
	if len(c.Parents) > MaxParents {
		return fmt.Errorf("commit has %d parents, more than %d", len(c.Parents), MaxParents)
	}
	seen := make(map[key.Key]struct{}, len(c.Parents))
	for i, p := range c.Parents {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("commit parent %d: %w", i, err)
		}
		if p.Type() != key.Commit {
			return fmt.Errorf("commit parent %d: %s is not a commit key (type %v)", i, p, p.Type())
		}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("commit parent %d: duplicate %s", i, p)
		}
		seen[p] = struct{}{}
	}
	if err := c.Author.validate(); err != nil {
		return fmt.Errorf("commit author: %w", err)
	}
	if err := c.Committer.validate(); err != nil {
		return fmt.Errorf("commit committer: %w", err)
	}
	if len(c.Message) > MaxMessageLen {
		return fmt.Errorf("commit message exceeds %d bytes", MaxMessageLen)
	}
	if !utf8.ValidString(c.Message) {
		return errors.New("commit message must be valid UTF-8")
	}
	if len(c.Signature) > MaxSignatureLen {
		return fmt.Errorf("commit signature exceeds %d bytes", MaxSignatureLen)
	}
	if len(c.PublicKey) > MaxPublicKeyLen {
		return fmt.Errorf("commit public key exceeds %d bytes", MaxPublicKeyLen)
	}
	if len(c.ChangeID) > MaxChangeIDLen {
		return fmt.Errorf("commit change id exceeds %d bytes", MaxChangeIDLen)
	}
	if n := len(c.ConflictTerms); n%2 != 0 || n > MaxConflictTerms {
		return fmt.Errorf("commit has %d conflict terms, want an even number up to %d: a remove for every further add", n, MaxConflictTerms)
	}
	for i, t := range c.ConflictTerms {
		if err := t.Validate(); err != nil {
			return fmt.Errorf("commit conflict term %d: %w", i, err)
		}
		if typ := t.Type(); typ != key.DirLeaf && typ != key.DirNode {
			return fmt.Errorf("commit conflict term %d: %s is not a directory key (type %v)", i, t, typ)
		}
	}
	if len(c.ConflictLabels) > 0 {
		if !c.Conflicted() {
			return errors.New("commit has conflict labels but no conflict")
		}
		if want := 1 + len(c.ConflictTerms); len(c.ConflictLabels) != want {
			return fmt.Errorf("commit has %d conflict labels for %d terms", len(c.ConflictLabels), want)
		}
		labelled := false
		for i, l := range c.ConflictLabels {
			if err := validateText(l, MaxLabelLen); err != nil {
				return fmt.Errorf("commit conflict label %d %w", i, err)
			}
			labelled = labelled || l != ""
		}
		if !labelled {
			return errors.New("commit conflict labels are all empty; an unlabelled conflict carries none")
		}
	}
	return nil
}

// Encode returns the deterministic CBOR encoding of a validated commit.
func (c Commit) Encode() ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	return encMode.Marshal(c.wire())
}

// Object encodes c and derives its key: type Commit, the length field a
// footprint as a directory's is — the encoding's own byte length plus the
// length of every tree (Trees). Parents are not counted: a parent's length
// would hold its own parents', so every merge would count the history its
// parents share twice, roughly doubling the value until it no longer fit;
// and a directory that holds a commit reports, through this length, the
// size of what is beneath it, not of its history.
func (c Commit) Object() (key.Key, []byte, error) {
	b, err := c.Encode()
	if err != nil {
		return key.Key{}, nil, err
	}
	length, err := Footprint(uint64(len(b)), c.Trees())
	if err != nil {
		return key.Key{}, nil, err
	}
	k, err := key.New(key.Commit, length, b)
	if err != nil {
		return key.Key{}, nil, err
	}
	return k, b, nil
}

// Footprint is the length field of the key of a commit whose encoding is own
// bytes long and which records trees. It is an error for the sum not to fit.
func Footprint(own uint64, trees []key.Key) (uint64, error) {
	length := own
	for _, t := range trees {
		sum := length + t.Length()
		if sum < length {
			return 0, errors.New("commit footprint overflows the key's length field")
		}
		length = sum
	}
	return length, nil
}

// SignaturePayload returns the bytes a signature runs over: the deterministic
// encoding without the Signature field. PublicKey stays in, so the payload
// binds the signer's key; set it before computing the payload.
func (c Commit) SignaturePayload() ([]byte, error) {
	c.Signature = nil
	return c.Encode()
}

// Decode parses and validates a commit. It rejects non-canonical encodings:
// the input must be byte-for-byte what Encode produces for the same record
// (extra or missing map keys, reordered keys, indefinite-length items,
// non-minimal integers and trailing bytes are all rejected).
func Decode(b []byte) (Commit, error) {
	var w wireCommit
	if err := cbor.Unmarshal(b, &w); err != nil {
		return Commit{}, fmt.Errorf("decoding commit: %w", err)
	}
	c, err := w.commit()
	if err != nil {
		return Commit{}, fmt.Errorf("invalid commit: %w", err)
	}
	if err := c.validate(); err != nil {
		return Commit{}, fmt.Errorf("invalid commit: %w", err)
	}
	canonical, err := encMode.Marshal(c.wire())
	if err != nil {
		return Commit{}, fmt.Errorf("re-encoding commit: %w", err)
	}
	if !bytes.Equal(canonical, b) {
		return Commit{}, errors.New("commit encoding is not canonical")
	}
	return c, nil
}
