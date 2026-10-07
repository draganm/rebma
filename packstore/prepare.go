package packstore

import (
	"fmt"

	"github.com/draganm/rebma/amberpack"
)

// prepare returns the record to append for obj and the payload length the
// write stats charge for it. For Data that is EncodeRecord's output after
// the optional verification; for a pre-encoded Record it is the record
// itself, after ParseRecord (framing, flags, length invariants, CRC,
// canonical key), a check that the record names obj.Key and is exactly one
// record long, and, with verify, a decode and rehash of the payload. Every
// rejection of a Record wraps ErrCorrupt, a verification failure ErrVerify.
func prepare(obj Object, verify bool) ([]byte, int64, error) {
	if obj.Record == nil {
		if verify {
			if err := verifyObject(obj); err != nil {
				return nil, 0, err
			}
		}
		rec, err := amberpack.EncodeRecord(obj.Key, obj.Data)
		if err != nil {
			return nil, 0, err
		}
		return rec, int64(len(obj.Data)), nil
	}
	if obj.Data != nil {
		return nil, 0, fmt.Errorf("%w: object %s carries both Data and Record", ErrCorrupt, obj.Key)
	}
	rec, err := amberpack.ParseRecord(obj.Record)
	if err != nil {
		return nil, 0, err
	}
	if rec.Key != obj.Key {
		return nil, 0, fmt.Errorf("%w: record key %s does not match %s", ErrCorrupt, rec.Key, obj.Key)
	}
	if len(obj.Record) != amberpack.RecHeaderSize+int(rec.Slen) {
		return nil, 0, fmt.Errorf("%w: record is %d bytes, want %d", ErrCorrupt, len(obj.Record), amberpack.RecHeaderSize+int(rec.Slen))
	}
	if verify {
		data, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, obj.Record[amberpack.RecHeaderSize:])
		if err != nil {
			return nil, 0, err
		}
		if err := verifyObject(Object{Key: obj.Key, Data: data}); err != nil {
			return nil, 0, err
		}
	}
	return obj.Record, int64(rec.Ulen), nil
}
