package reference_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/reference"
)

// testKey returns a valid canonical key to point references at.
func testKey(t *testing.T) []byte {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	return o.Key[:]
}

func TestValidateName(t *testing.T) {
	long := strings.Repeat("x", 1025)
	ok := strings.Repeat("y", 1024)
	cases := []struct {
		name    string
		refName string
		wantErr bool
	}{
		{"simple", "backup", false},
		{"with slash", "backups/2026/06", false},
		{"dotdot segment", "a/../b", false},
		{"empty segment", "a//b", false},
		{"unicode", "snapshot-éñ", false},
		{"max length", ok, false},
		{"empty", "", true},
		{"too long", long, true},
		{"at sign", "a@b", true},
		{"control char", "a\x01b", true},
		{"del char", "a\x7fb", true},
		{"newline", "a\nb", true},
		{"invalid utf8", string([]byte{0xff, 0xfe}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := reference.ValidateName(tc.refName)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateName(%q) error = %v, wantErr %v", tc.refName, err, tc.wantErr)
			}
		})
	}
}

func TestValidateUser(t *testing.T) {
	long := strings.Repeat("u", 1025)
	cases := []struct {
		name    string
		user    string
		wantErr bool
	}{
		{"simple name", "alice", false},
		{"email address", "alice@example.com", false},
		{"at sign accepted", "user@host", false},
		{"max length", strings.Repeat("u", 1024), false},
		{"empty rejected", "", true},
		{"too long rejected", long, true},
		{"control char rejected", "a\x01b", true},
		{"newline rejected", "a\nb", true},
		{"del char rejected", "a\x7fb", true},
		{"invalid utf8 rejected", string([]byte{0xff, 0xfe}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := reference.ValidateUser(tc.user)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateUser(%q) error = %v, wantErr %v", tc.user, err, tc.wantErr)
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := reference.Reference{
		Name:      "backups/home",
		Key:       testKey(t),
		User:      "dragan",
		CreatedAt: 1765432100123456789,
		Signature: []byte{1, 2, 3},
		PublicKey: []byte{4, 5, 6},
	}
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := reference.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != r.Name || !bytes.Equal(got.Key, r.Key) || got.User != r.User ||
		got.CreatedAt != r.CreatedAt || !bytes.Equal(got.Signature, r.Signature) ||
		!bytes.Equal(got.PublicKey, r.PublicKey) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, r)
	}
}

func TestEncodeDeterministic(t *testing.T) {
	r := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42}
	a, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("two encodes of the same record differ")
	}
}

func TestSignaturePayloadExcludesSignature(t *testing.T) {
	unsigned := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42}
	signed := unsigned
	signed.Signature = []byte{9, 9, 9}

	unsignedEnc, err := unsigned.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := signed.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, unsignedEnc) {
		t.Fatal("SignaturePayload differs from the encoding of the unsigned record")
	}
}

func TestSignaturePayloadIncludesPublicKey(t *testing.T) {
	withKey := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42, PublicKey: []byte{7, 7, 7}}
	signed := withKey
	signed.Signature = []byte{9, 9, 9}

	withKeyEnc, err := withKey.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := signed.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, withKeyEnc) {
		t.Fatal("SignaturePayload differs from the encoding of the unsigned record with its public key")
	}

	withoutKey := withKey
	withoutKey.PublicKey = nil
	withoutKeyEnc, err := withoutKey.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(payload, withoutKeyEnc) {
		t.Fatal("SignaturePayload does not cover the public key")
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	if _, err := (reference.Reference{Name: "a@b", Key: testKey(t)}).Encode(); err == nil {
		t.Fatal("expected error for invalid name")
	}
	if _, err := (reference.Reference{Name: "ok", Key: []byte{1, 2}}).Encode(); err == nil {
		t.Fatal("expected error for non-canonical key")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := reference.Decode([]byte("not cbor at all")); err == nil {
		t.Fatal("expected error for garbage input")
	}
}

// goldenHex is the canonical CBOR encoding of Reference{Name:"n", Key:<EncodeBlob("hello")>, User:"u", CreatedAt:42}.
// It pins the wire format so a CBOR library upgrade cannot silently change encodings that
// signatures and persistence depend on.
const goldenHex = "a400616e015820674a62c508e9868ab74ec1f86e50b34b8de5c591445e928286b33d168fea050002617503182a"

func TestGoldenVector(t *testing.T) {
	r := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42}
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString(goldenHex)
	if !bytes.Equal(b, want) {
		t.Fatalf("encoding changed!\n got: %s\nwant: %s", hex.EncodeToString(b), goldenHex)
	}
}

func TestDecodeRejectsExtraMapKey(t *testing.T) {
	// Golden bytes with map header changed from a4 (4 items) to a5 (5 items)
	// and an extra key-value pair (09 = int 9, 61 78 = text "x") appended.
	extra, _ := hex.DecodeString("a500616e015820674a62c508e9868ab74ec1f86e50b34b8de5c591445e928286b33d168fea050002617503182a096178")
	if _, err := reference.Decode(extra); err == nil {
		t.Fatal("expected error for encoding with extra map key")
	}
}

func TestEncodeRejectsTooLongUserAndSignature(t *testing.T) {
	k := testKey(t)

	// User exactly at the limit is accepted.
	okUser := strings.Repeat("u", reference.MaxUserLen)
	if _, err := (reference.Reference{Name: "n", Key: k, User: okUser}).Encode(); err != nil {
		t.Fatalf("user at MaxUserLen should be accepted: %v", err)
	}

	// User one byte over the limit is rejected.
	longUser := strings.Repeat("u", reference.MaxUserLen+1)
	if _, err := (reference.Reference{Name: "n", Key: k, User: longUser}).Encode(); err == nil {
		t.Fatal("expected error for user exceeding MaxUserLen")
	}

	// Signature exactly at the limit is accepted.
	okSig := make([]byte, reference.MaxSignatureLen)
	if _, err := (reference.Reference{Name: "n", Key: k, Signature: okSig}).Encode(); err != nil {
		t.Fatalf("signature at MaxSignatureLen should be accepted: %v", err)
	}

	// Signature one byte over the limit is rejected.
	longSig := make([]byte, reference.MaxSignatureLen+1)
	if _, err := (reference.Reference{Name: "n", Key: k, Signature: longSig}).Encode(); err == nil {
		t.Fatal("expected error for signature exceeding MaxSignatureLen")
	}

	// Public key exactly at the limit is accepted.
	okPub := make([]byte, reference.MaxPublicKeyLen)
	if _, err := (reference.Reference{Name: "n", Key: k, PublicKey: okPub}).Encode(); err != nil {
		t.Fatalf("public key at MaxPublicKeyLen should be accepted: %v", err)
	}

	// Public key one byte over the limit is rejected.
	longPub := make([]byte, reference.MaxPublicKeyLen+1)
	if _, err := (reference.Reference{Name: "n", Key: k, PublicKey: longPub}).Encode(); err == nil {
		t.Fatal("expected error for public key exceeding MaxPublicKeyLen")
	}
}

func TestDecodeRejectsNonCanonicalEncoding(t *testing.T) {
	// Golden bytes with the last two bytes (18 2a = uint 42) replaced by
	// 19 00 2a (two-byte encoding of 42) — valid CBOR but not minimal.
	nonCanon, _ := hex.DecodeString("a400616e015820674a62c508e9868ab74ec1f86e50b34b8de5c591445e928286b33d168fea05000261750319002a")
	if _, err := reference.Decode(nonCanon); err == nil {
		t.Fatal("expected error for non-canonical (non-minimal integer) encoding")
	}
}
