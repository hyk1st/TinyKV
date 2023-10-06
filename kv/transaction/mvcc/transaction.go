package mvcc

import (
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
// MvccTxn 将写入作为单个事务的一部分进行分组。它还为底层 存储的抽象，将时间戳、写入和锁的概念简化为简单的键和值。
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	key = EncodeKey(key, ts)
	tempPut := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	}
	txn.writes = append(txn.writes, tempPut)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lockByte, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if len(lockByte) == 0 {
		return nil, nil
	}
	return ParseLock(lockByte)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	tempPut := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, tempPut)

}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	tempDel := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, tempDel)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// GetValue 查找键的值，该值在此事务的起始时间戳有效。
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	stKey := EncodeKey(key, txn.StartTS)
	iter.Seek(stKey)
	for iter.Valid() {
		if !reflect.DeepEqual(DecodeUserKey(iter.Item().Key()), key) {
			break
		}
		val, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(val)
		if err != nil || write.Kind == WriteKindDelete {
			return nil, err
		}
		if write.Kind == WriteKindRollback {
			iter.Next()
			continue
		}
		if write.Kind == WriteKindPut && write.StartTS < txn.StartTS {
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		}
		iter.Next()
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	tempPut := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, tempPut)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	tempDel := storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, tempDel)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// CurrentWrite 搜索具有该事务开始时间戳的写入。它会从数据库返回写入信息和该写入信息的写入的提交时间戳，或者返回错误。
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	enKey := EncodeKey(key, TsMax)
	iter.Seek(enKey)
	for iter.Valid() {
		if !reflect.DeepEqual(DecodeUserKey(iter.Item().Key()), key) {
			break
		}
		commitTime := decodeTimestamp(iter.Item().Key())
		val, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS < txn.StartTS {
			break
		}
		if write.StartTS == txn.StartTS {
			return write, commitTime, nil
		}
		iter.Next()
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	enKey := EncodeKey(key, TsMax)
	iter.Seek(enKey)
	for iter.Valid() {
		if !reflect.DeepEqual(DecodeUserKey(iter.Item().Key()), key) {
			break
		}
		commitTime := decodeTimestamp(iter.Item().Key())
		val, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		return write, commitTime, nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
