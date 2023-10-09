package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"reflect"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// 扫描器用于从存储层读取多个顺序键/值对。它了解存储层的实现，并返回适合用户的结果。
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
// 不变式：要么扫描仪已完成，不能使用，要么扫描仪已准备好立即返回值。
type Scanner struct {
	// Your Data Here (4C).
	CommitIter  engine_util.DBIterator
	DefaultIter engine_util.DBIterator
	StartTs     uint64
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).

	scan := &Scanner{
		CommitIter:  txn.Reader.IterCF(engine_util.CfWrite),
		DefaultIter: txn.Reader.IterCF(engine_util.CfDefault),
		StartTs:     txn.StartTS,
	}
	scan.CommitIter.Seek(EncodeKey(startKey, txn.StartTS))
	scan.DefaultIter.Seek(EncodeKey(startKey, txn.StartTS))
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.CommitIter.Close()
	scan.DefaultIter.Close()

}

func (scan *Scanner) findNextKey() {
	key := scan.CommitIter.Item().Key()
	key = EncodeKey(DecodeUserKey(key), 0)
	scan.CommitIter.Seek(key)
}

func (scan *Scanner) getKeyWithStartTS() {
	key := scan.CommitIter.Item().Key()
	key = EncodeKey(DecodeUserKey(key), scan.StartTs)
	scan.CommitIter.Seek(key)
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.CommitIter.Valid() {
		value, err := scan.CommitIter.Item().Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}
		if decodeTimestamp(scan.CommitIter.Item().Key()) > scan.StartTs {
			scan.getKeyWithStartTS()
			continue
		}
		switch write.Kind {
		case WriteKindDelete:
			scan.findNextKey()
			continue
		case WriteKindRollback:
			scan.CommitIter.Next()
			continue
		}
		key := scan.CommitIter.Item().Key()
		encodeKey := EncodeKey(DecodeUserKey(key), scan.StartTs)
		scan.DefaultIter.Seek(encodeKey)
		if reflect.DeepEqual(scan.DefaultIter.Item().Key(), encodeKey) {
			key = make([]byte, 0)
			value = make([]byte, 0)
			key = scan.DefaultIter.Item().KeyCopy(key)
			value, _ = scan.DefaultIter.Item().ValueCopy(value)
			scan.findNextKey()
			return DecodeUserKey(key), value, nil
		}
		encodeKey = EncodeKey(DecodeUserKey(key), write.StartTS)
		scan.DefaultIter.Seek(encodeKey)
		if reflect.DeepEqual(scan.DefaultIter.Item().Key(), encodeKey) {
			key = make([]byte, 0)
			value = make([]byte, 0)
			key = scan.DefaultIter.Item().KeyCopy(key)
			value, _ = scan.DefaultIter.Item().ValueCopy(value)
			scan.findNextKey()
			return DecodeUserKey(key), value, nil
		}
		scan.CommitIter.Next()
	}
	return nil, nil, nil
}
