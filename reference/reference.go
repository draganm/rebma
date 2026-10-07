// Package reference defines the named-pointer record: a global name pointing
// at a store key, with creator, creation time, and an optional opaque
// signature. Encoding is RFC 8949 §4.2 core-deterministic CBOR, matching the
// fstree object convention (canonical map, integer keys).
package reference

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/draganm/rebma/key"
	"github.com/fxamacker/cbor/v2"
)

// MaxNameLen is the maximum reference name length in bytes.
const MaxNameLen = 1024

// MaxUserLen is the maximum User field length in bytes.
const MaxUserLen = 1024

// MaxSignatureLen is the maximum Signature field length in bytes (64 KiB).
const MaxSignatureLen = 64 << 10

// MaxPublicKeyLen is the maximum PublicKey field length in bytes (16 KiB).
const MaxPublicKeyLen = 16 << 10

// encMode is the shared deterministic encoder, mirroring fstree.encMode.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("reference: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Reference is the record stored under a name. Fields are encoded as a
// canonical CBOR map with integer keys 0-5; Signature (key 4) and PublicKey
// (key 5) are omitted when absent. The signature payload is the encoding
// without key 4 only, so a signature covers the public key it was made with.
type Reference struct {
	Name      string `cbor:"0,keyasint"`
	Key       []byte `cbor:"1,keyasint"` // 32-byte canonical store key
	User      string `cbor:"2,keyasint"`
	CreatedAt int64  `cbor:"3,keyasint"` // ns since the Unix epoch
	Signature []byte `cbor:"4,keyasint,omitempty"`
	PublicKey []byte `cbor:"5,keyasint,omitempty"` // signer's key, SSH wire format
}

// ValidateName checks the reference-name rules: 1..MaxNameLen bytes of valid
// UTF-8, no '@' (the ref/path separator) and no control characters. '/' is
// allowed; names are opaque strings with no structural meaning.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("reference name must not be empty")
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("reference name exceeds %d bytes", MaxNameLen)
	}
	if !utf8.ValidString(name) {
		return errors.New("reference name must be valid UTF-8")
	}
	for _, r := range name {
		if r == '@' {
			return errors.New("reference name must not contain '@'")
		}
		if r < 0x20 || r == 0x7f {
			return errors.New("reference name must not contain control characters")
		}
	}
	return nil
}

// ValidateUser checks the user-identity rules used both by config-user and by
// Reference records: 1..MaxUserLen bytes of valid UTF-8 with no control
// characters. '@' is explicitly allowed so that e-mail addresses are valid.
//
// Note: an empty User in a Reference record remains valid at the record level
// (ValidateUser is only called from validate() when r.User != ""). ValidateUser
// itself rejects empty so that config-user always stores a usable identity.
func ValidateUser(user string) error {
	if user == "" {
		return errors.New("user must not be empty")
	}
	if len(user) > MaxUserLen {
		return fmt.Errorf("user exceeds %d bytes", MaxUserLen)
	}
	if !utf8.ValidString(user) {
		return errors.New("user must be valid UTF-8")
	}
	for _, r := range user {
		if r < 0x20 || r == 0x7f {
			return errors.New("user must not contain control characters")
		}
	}
	return nil
}

// validate checks the whole record: name rules plus a canonical key, and
// bounds on the User and Signature fields.
func (r Reference) validate() error {
	if err := ValidateName(r.Name); err != nil {
		return err
	}
	if _, err := key.Parse(r.Key); err != nil {
		return fmt.Errorf("reference key: %w", err)
	}
	if r.User != "" {
		if err := ValidateUser(r.User); err != nil {
			return fmt.Errorf("reference user: %w", err)
		}
	}
	if len(r.Signature) > MaxSignatureLen {
		return fmt.Errorf("reference signature exceeds %d bytes", MaxSignatureLen)
	}
	if len(r.PublicKey) > MaxPublicKeyLen {
		return fmt.Errorf("reference public key exceeds %d bytes", MaxPublicKeyLen)
	}
	return nil
}

// Encode returns the deterministic CBOR encoding of a validated record.
func (r Reference) Encode() ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return encMode.Marshal(r)
}

// Decode parses and validates a record. It rejects non-canonical encodings:
// the input must be byte-for-byte identical to what encMode would produce for
// the same record (extra map keys, indefinite-length items, and non-minimal
// integer/length encodings are all rejected).
func Decode(b []byte) (Reference, error) {
	var r Reference
	if err := cbor.Unmarshal(b, &r); err != nil {
		return Reference{}, fmt.Errorf("decoding reference: %w", err)
	}
	if err := r.validate(); err != nil {
		return Reference{}, fmt.Errorf("invalid reference: %w", err)
	}
	canonical, err := encMode.Marshal(r)
	if err != nil {
		return Reference{}, fmt.Errorf("re-encoding reference: %w", err)
	}
	if !bytes.Equal(canonical, b) {
		return Reference{}, fmt.Errorf("reference encoding is not canonical")
	}
	return r, nil
}

// SignaturePayload returns the bytes a signature runs over: the deterministic
// encoding of the record without its Signature field. PublicKey stays in, so
// the payload binds the signer's key; set it before computing the payload.
func (r Reference) SignaturePayload() ([]byte, error) {
	r.Signature = nil
	return r.Encode()
}
