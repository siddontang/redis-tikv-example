package main

import (
	"context"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

func HandleSet(db kv.Storage, key []byte, value []byte) (interface{}, error) {
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	it, err := SeekPrefix(txn, key)
	if err != nil {
		return nil, err
	}

	if it != nil {
		defer it.Close()
		if tp := GetKeyType(it.Key()); tp != String {
			return nil, errors.Errorf("invalid type, need %d, but got %d", String, tp)
		}
	}

	if err := txn.Set(EncodeStringKey(nil, key), value); err != nil {
		return nil, err
	}

	if err := txn.Commit(context.TODO()); err != nil {
		return nil, err
	}

	return OK, nil
}

func HandleGet(db kv.Storage, key []byte) (interface{}, error) {
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

	if tp := GetKeyType(it.Key()); tp != String {
		return nil, errors.Errorf("invalid type, need %d, but got %d", String, tp)
	}

	value := append([]byte(nil), it.Value()...)

	if err := txn.Commit(context.TODO()); err != nil {
		return nil, nil
	}

	return value, nil
}
