package main

import (
	"context"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

func HandleHashSet(db kv.Storage, key []byte, field []byte, value []byte) (interface{}, error) {
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	it, err := SeekPrefix(txn, key)
	if err != nil {
		return nil, err
	}

	num := int64(1)
	fieldKey := EncodeHashField(nil, key, field)
	metaKey := EncodeHashMetaKey(nil, key)

	if it != nil {
		defer it.Close()

		if tp := GetKeyType(it.Key()); tp != HashMeta {
			return nil, errors.Errorf("invalid type, need %d, but got %d", String, tp)
		}

		num, err = strconv.ParseInt(string(it.Value()), 10, 64)
		if err != nil {
			return nil, err
		}

		value, err := txn.Get(fieldKey)
		if err != nil && !kv.ErrNotExist.Equal(err) {
			return nil, err
		}

		if value == nil {
			num++
		}
	}

	if err := txn.Set(metaKey, []byte(strconv.FormatInt(num, 10))); err != nil {
		return nil, err
	}

	if err := txn.Set(fieldKey, value); err != nil {
		return nil, err
	}

	if err := txn.Commit(context.TODO()); err != nil {
		return nil, err
	}

	return num, nil
}

func HandleHashGet(db kv.Storage, key []byte, field []byte) (interface{}, error) {
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	it, err := SeekPrefix(txn, key)
	if err != nil || it == nil {
		return nil, err
	}
	defer it.Close()

	if tp := GetKeyType(it.Key()); tp != HashMeta {
		return nil, errors.Errorf("invalid type, need %d, but got %d", String, tp)
	}

	value, err := txn.Get(EncodeHashField(nil, key, field))
	if err != nil {
		return nil, err
	}

	if err := txn.Commit(context.TODO()); err != nil {
		return nil, nil
	}

	return append([]byte(nil), value...), nil
}
