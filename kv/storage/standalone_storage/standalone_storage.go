package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	*badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	option := badger.DefaultOptions
	option.Dir = conf.DBPath
	option.ValueDir = conf.DBPath
	standAloneStorage, err := badger.Open(option)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{
		standAloneStorage,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.DB == nil {
		return errors.New("badger.DB is not open success")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.DB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return storage.BadgerReader{Txn: s.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.DB.NewTransaction(true)
	var err error
	for _, kv := range batch {
		if err = txn.Set(append([]byte(kv.Cf()+"_"), kv.Key()...), kv.Value()); err != nil {
			txn.Discard()
			return err
		}
	}
	return txn.Commit()
}
