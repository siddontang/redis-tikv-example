package main

import (
	"context"
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

var (
	emptyBytes       = make([]byte, 0)
	paddingZeros     = make([]byte, 9)
	errCorruptedData = errors.New("Failed to decode corrupted data")
)

const (
	rdbEscapeLength = 9
	signMask        = 0x8000000000000000
)

func rdbEncodedSize(len int) int {
	return (1 + ((len + (rdbEscapeLength - 2)) / (rdbEscapeLength - 1))) * rdbEscapeLength
}

func ensureCapacity(from []byte, extra int) []byte {
	newSize := len(from) + extra
	if cap(from) < newSize {
		to := make([]byte, len(from), newSize)
		copy(to, from)
		return to
	}
	return from
}

// EncodeBytes encodes the slice to buffer with mem-comparable format from myrocks
func EncodeBytes(buffer []byte, src []byte) []byte {
	buffer = ensureCapacity(buffer, rdbEncodedSize(len(src)))
	for {
		// Figure out how many bytes to copy, copy them and adjust pointers
		var copyLen int
		srcLen := len(src)
		if rdbEscapeLength-1 < srcLen {
			copyLen = rdbEscapeLength - 1
		} else {
			copyLen = srcLen
		}
		buffer = append(buffer, src[0:copyLen]...)
		src = src[copyLen:]

		// Are we at the end of the input?
		if len(src) == 0 {
			// pad with zeros if necessary;
			paddingBytes := rdbEscapeLength - 1 - copyLen
			if paddingBytes > 0 {
				buffer = append(buffer, paddingZeros[0:paddingBytes]...)
			}

			// Put the flag byte (0 - N-1) in the output
			buffer = append(buffer, byte(copyLen))
			break
		}

		// We have more data - put the flag byte (N) in and continue
		buffer = append(buffer, rdbEscapeLength)
	}

	return buffer
}

// DecodeBytes decodes the slice, return the left buffer, decoded slice or error
func DecodeBytes(from []byte) ([]byte, []byte, error) {
	data := make([]byte, 0, len(from))
	for {
		if len(from) < rdbEscapeLength {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := from[:rdbEscapeLength]
		group := groupBytes[:rdbEscapeLength-1]
		realGroupSize := groupBytes[rdbEscapeLength-1]
		if realGroupSize > rdbEscapeLength {
			return nil, nil, errors.Errorf("invalid flag byte, group bytes %q", groupBytes)
		}

		from = from[rdbEscapeLength:]

		if realGroupSize < rdbEscapeLength {
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != 0 {
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			data = append(data, group[:realGroupSize]...)
			break
		} else {
			data = append(data, group[:rdbEscapeLength-1]...)
		}
	}
	return from, data, nil
}

// EncodeInt64 encodes the int64 to buffer with mem-comparable format.
func EncodeInt64(buffer []byte, i int64) []byte {
	var data [8]byte
	u := uint64(i) ^ signMask
	binary.BigEndian.PutUint64(data[:], u)
	return append(buffer, data[:]...)
}

// DecodeInt64 decodes the int64, return the left buffer, decoded int64 or error.
func DecodeInt64(from []byte) ([]byte, int64, error) {
	if len(from) < 8 {
		return nil, 0, errCorruptedData
	}
	u := binary.BigEndian.Uint64(from[:8])
	return from[8:], int64(u ^ signMask), nil
}

func GetKeyType(key []byte) uint8 {
	return key[len(key)-1]
}

func EncodeStringKey(buf []byte, key []byte) []byte {
	key = EncodeBytes(buf, key)
	buf = append(buf, key...)
	buf = append(buf, String)
	return buf
}

func EncodeHashMetaKey(buf []byte, key []byte) []byte {
	key = EncodeBytes(buf, key)
	buf = append(buf, key...)
	buf = append(buf, HashMeta)
	return buf
}

func EncodeHashField(buf []byte, key []byte, field []byte) []byte {
	key = EncodeBytes(buf, key)
	buf = append(buf, key...)
	buf = append(buf, HashField)
	field = EncodeBytes(buf, field)
	buf = append(buf, field...)
	return buf
}

func HandleDelete(db kv.Storage, key []byte) (interface{}, error) {
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	prefixKey := EncodeBytes(nil, key)
	it, err := txn.Seek(prefixKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var keys [][]byte
	for it.Valid() && it.Key().HasPrefix(prefixKey) {
		keys = append(keys, it.Key().Clone())
		if err := it.Next(); err != nil {
			return nil, err
		}
	}

	hasKey := int64(0)
	for _, key := range keys {
		hasKey = 1
		if err := txn.Delete(key); err != nil {
			return nil, err
		}
	}

	if err := txn.Commit(context.TODO()); err != nil {
		return nil, err
	}

	return hasKey, nil
}

func SeekPrefix(txn kv.Transaction, key []byte) (kv.Iterator, error) {
	seekKey := EncodeBytes(nil, key)
	it, err := txn.Seek(seekKey)
	if err != nil {
		return nil, err
	}

	if !it.Valid() {
		it.Close()
		return nil, nil
	}

	if !it.Key().HasPrefix(key) {
		it.Close()
		return nil, nil
	}

	return it, nil
}
