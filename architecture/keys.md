# Lookup Keys

A lookup key identifies a piece of content in the store. It encodes the **type** of the payload and a **hash** of it.

A rebma key is an [Amber-Store](https://github.com/amber-store/core) key with its **bytes reversed**: byte `i` of one is byte `31 - i` of the other. The fields and their meaning are Amber's; only their position in the key differs. The hash therefore comes first, so a key's leading bytes are uniformly distributed, and the type and length sit at the end.

Every key is exactly **32 bytes** long, laid out as three contiguous fields:

| Field | Size | Purpose |
|-------|------|---------|
| Payload hash   | remaining bytes       | Truncated Blake3 hash of the content, byte-reversed |
| Payload length | 1–8 bytes             | Little-endian byte length of the content |
| Header byte    | 1 byte (the last)     | Payload type and length-field size |

Because the total is fixed at 32 bytes, the three fields trade space against one another: the longer the payload-length field, the fewer bytes remain for the hash.

For example, a `DirNode` (type 3) of length 1000 (`0x03e8`) whose digest starts `01 02 03 … 1d`:

```
1d1c1b1a191817161514131211100f0e0d0c0b0a090807060504030201  e803  31
└─ 29 digest bytes, last first ──────────────────────────┘  │     └─ header: type 3, 2 length bytes
                                                            └─ length 1000, little-endian
```

The same object's Amber-Store key is `31 03e8 0102…1d`.

## Header byte

The header byte is the **last** byte of the key. It is split into three fields, from most- to least-significant bit:

- **4 bits — type:** describes the kind of payload (e.g. Tree, Blob etc.).
- **1 bit — reserved:** must always be `0`.
- **3 bits — length size:** the number of bytes used by the payload-length field. The stored value is offset by one: `0` means 1 byte, `1` means 2 bytes, …, `7` means 8 bytes. So the field is always 1–8 bytes long.

## Payload length

A Little-Endian encoding of the total byte length of the content, in the bytes just before the header byte. (It is Amber's big-endian field, reversed with the rest of the key.)

Several combinations of length value and length-field size are semantically equivalent, so a canonical encoding is enforced: the last, most significant byte of the payload-length field — the one next to the header byte — must never be `0` (no zero padding). The one exception is a zero length, encoded as a single `0` byte.

There is a special case when the object type is a directory, the payload length will represent cumulative length of data in the whole subtree. A commit follows the same idea: its payload length is its own byte length plus the lengths of the trees it records, and its parent commits are not counted ([commits.md](commits.md#the-key)). [types.md](types.md#length-field-logical-size-not-serialized-size) states the rule for every type.

## Payload hash

The payload hash is the [Blake3](https://github.com/BLAKE3-team/BLAKE3) hash of the content, truncated to fill the remaining space in the key:

```
hash length = 32 - 1 - <length-field size>
```

That is, 32 total bytes minus the 1 header byte minus however many bytes the payload-length field occupies.

The digest is truncated to its **leading** bytes, as in Amber-Store, and those bytes are stored in **reverse order**: the key's first byte is the last digest byte kept, and the byte just before the payload length is the digest's first byte. `key.Key.Hash` returns them in digest order.

The hash is at least 23 bytes long, so the first bytes of a key are always hash bytes. The packstore relies on this: a sealed segment's index fans out on a key's first byte and its filter is keyed on the first eight.
