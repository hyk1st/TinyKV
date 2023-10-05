package storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type BadgerReader struct {
	*badger.Txn
}

func (reader BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	cf += "_"
	item, err := reader.Get(append([]byte(cf), key...))
	if err != nil {
		return nil, err
	}
	if item.ValueSize() == 0 {
		return nil, nil
	}
	resp := make([]byte, item.ValueSize())
	return item.ValueCopy(resp)
}
func (reader BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.Txn)
}
func (reader BadgerReader) Close() {
	reader.Commit()
}
