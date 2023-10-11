package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
	"reflect"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts != req.Version && lock.Ts+lock.Ttl < req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keys := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	resp := &kvrpcpb.PrewriteResponse{
		Errors: make([]*kvrpcpb.KeyError, 0),
	}
	for {
		wg := server.Latches.AcquireLatches(keys)
		if wg == nil {
			break
		}
		wg.Wait()
	}
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)

	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 检查是否键被提交
	for _, mutation := range req.Mutations {
		key := mutation.GetKey()
		write, time, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && time >= req.StartVersion {
			temp := kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: time,
					Key:        key,
				},
			}
			resp.Errors = append(resp.Errors, &temp)
		}
	}
	if len(resp.Errors) > 0 {
		return resp, nil
	}
	// 检查锁冲突
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			if lock.Ts != req.StartVersion {
				keyError.Conflict = &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: lock.Ts,
					Key:        key,
					Primary:    lock.Primary,
				}
			}
			resp.Errors = append(resp.Errors, keyError)
		}
	}
	if len(resp.Errors) > 0 {
		return resp, nil
	}
	Locks := make([]storage.Modify, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindPut,
		}
		Locks = append(Locks, storage.Modify{Data: storage.Put{
			Key:   mutation.Key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		}})
		txn.PutLock(mutation.Key, lock)
	}
	err = server.storage.Write(req.Context, Locks)
	if err != nil {
		return resp, err
	}
	modify := make([]storage.Modify, len(req.Mutations))
	for _, mutation := range req.Mutations {
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			modify = append(modify, storage.Modify{Data: storage.Put{
				Key:   mvcc.EncodeKey(mutation.Key, req.StartVersion),
				Value: mutation.Value,
				Cf:    engine_util.CfDefault,
			}})
		case kvrpcpb.Op_Del:
			modify = append(modify, storage.Modify{Data: storage.Delete{
				Key: mvcc.EncodeKey(mutation.Key, req.StartVersion),
				Cf:  engine_util.CfDefault,
			}})
		}
	}
	err = server.storage.Write(req.Context, modify)

	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	for {
		wg := server.Latches.AcquireLatches(req.Keys)
		if wg == nil {
			break
		}
		wg.Wait()
	}
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.CommitResponse{}
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 检查是否已提交或回滚
	for _, key := range req.Keys {
		write, time, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && time > req.StartVersion {
			if write.StartTS == req.StartVersion && write.Kind != 3 {
				return resp, nil
			}
			resp.Error = &kvrpcpb.KeyError{}
			return resp, nil
		}
	}

	// 检查锁的状态
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			if lock.Ts != req.StartVersion {
				resp.Error.Conflict = &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: lock.Ts,
					Key:        key,
					Primary:    lock.Primary,
				}
			}
			resp.Error.Retryable = "true"
			return resp, nil
		}
	}
	defer func() {
		for _, key := range req.Keys {
			server.DeleteLock(key, req.Context)
			txn.DeleteLock(key)
		}
	}()
	modify := make([]storage.Modify, len(req.Keys))
	for _, key := range req.Keys {
		modify = append(modify, storage.Modify{Data: storage.Put{
			Key: mvcc.EncodeKey(key, req.CommitVersion),
			Value: (&mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindPut,
			}).ToBytes(),
			Cf: engine_util.CfWrite,
		}})
	}
	err = server.storage.Write(req.Context, modify)

	return resp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.ScanResponse{
		Pairs: make([]*kvrpcpb.KvPair, 0),
	}
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scan := mvcc.NewScanner(req.StartKey, txn)

	for req.Limit > 0 {
		key, value, err := scan.Next()
		if err != nil {
			continue
		}
		if key == nil || value == nil {
			break
		}
		req.Limit--
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		})
	}
	return resp, nil
}

func (server *Server) getRecentWrite(key []byte, ts uint64, lockTS uint64, reader storage.StorageReader) ([]byte, *mvcc.Write, error) {
	iter := reader.IterCF(engine_util.CfWrite)
	iter.Seek(mvcc.EncodeKey(key, ts))
	var write *mvcc.Write = nil
	rkey := make([]byte, 0)
	for iter.Valid() && reflect.DeepEqual(mvcc.DecodeUserKey(iter.Item().Key()), key) {
		val, err := iter.Item().Value()
		if err != nil {
			return nil, nil, err
		}
		write, err = mvcc.ParseWrite(val)
		if err != nil {
			return nil, nil, err
		}
		if write.StartTS == lockTS {
			rkey = iter.Item().KeyCopy(rkey)
			return rkey, write, nil
		}
		iter.Next()
	}
	return nil, nil, nil
}

func (server *Server) checkTransCommitOrRoBack(req *kvrpcpb.CheckTxnStatusRequest, reader storage.StorageReader) (*kvrpcpb.CheckTxnStatusResponse, error) {

	for temp := server.Latches.AcquireLatches(append(make([][]byte, 0), req.PrimaryKey)); temp != nil; {
		temp.Wait()
	}
	defer server.Latches.ReleaseLatches(append(make([][]byte, 0), req.PrimaryKey))
	key, write, err := server.getRecentWrite(req.PrimaryKey, req.CurrentTs, req.LockTs, reader)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	if write == nil {
		lockByte, err := reader.GetCF(engine_util.CfLock, req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if lockByte == nil || len(lockByte) == 0 {
			server.RollBack(mvcc.EncodeKey(req.PrimaryKey, req.LockTs), req.LockTs, req.Context)
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
		lock, err := mvcc.ParseLock(lockByte)
		if err != nil {
			return nil, err
		}
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl > mvcc.PhysicalTime(req.CurrentTs) {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			server.DeleteVal(req.PrimaryKey, req.LockTs, req.Context)
			server.RollBack(mvcc.EncodeKey(req.PrimaryKey, req.LockTs), req.LockTs, req.Context)
			server.DeleteLock(req.PrimaryKey, req.Context)
			return resp, nil
		}
	}
	if write.Kind == mvcc.WriteKindRollback {
		lockByte, err := reader.GetCF(engine_util.CfLock, req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_NoAction
		if lockByte == nil || len(lockByte) == 0 {
			return resp, nil
		}
		lock, err := mvcc.ParseLock(lockByte)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.LockTs {
			server.DeleteLock(req.PrimaryKey, req.Context)
		}
		//server.RollBack(mvcc.EncodeKey(req.PrimaryKey, req.LockTs), req.LockTs, req.Context)
		resp.CommitVersion = 0
		resp.LockTtl = 0
		return resp, nil
	}
	resp.CommitVersion = mvcc.DecodeTimestamp(key)
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	return server.checkTransCommitOrRoBack(req, reader)

}

// 删除锁
func (server *Server) DeleteLock(key []byte, Context *kvrpcpb.Context) {
	modify := storage.Modify{Data: storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}}
	_ = server.storage.Write(Context, []storage.Modify{modify})
}

// 回滚
func (server *Server) RollBack(key []byte, startTs uint64, Context *kvrpcpb.Context) {
	write := mvcc.Write{
		StartTS: startTs,
		Kind:    mvcc.WriteKindRollback,
	}
	modify := storage.Modify{Data: storage.Put{
		Key:   key,
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}}
	_ = server.storage.Write(Context, append(make([]storage.Modify, 0), modify))
}

func (server *Server) DeleteVal(key []byte, ts uint64, Context *kvrpcpb.Context) {
	reader, _ := server.storage.Reader(Context)
	val, err := reader.GetCF(engine_util.CfDefault, mvcc.EncodeKey(key, ts))
	if val == nil || len(val) == 0 || err != nil {
		return
	}
	modifys := make([]storage.Modify, 0)

	modifys = append(modifys, storage.Modify{
		Data: storage.Delete{
			Key: mvcc.EncodeKey(key, ts),
			Cf:  engine_util.CfDefault,
		},
	})
	_ = server.storage.Write(nil, modifys)
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.BatchRollbackResponse{}
	mp := make(map[string]*mvcc.Write)
	// 检查是否有已提交的键
	for _, key := range req.Keys {
		_, write, err := server.getRecentWrite(key, mvcc.TsMax, req.StartVersion, reader)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{Abort: "true"}
			return resp, nil
		}
		mp[string(key)] = write
	}

	// 检查锁冲突
	for _, key := range req.Keys {
		lockByte, err := reader.GetCF(engine_util.CfLock, key)
		if err != nil {
			return nil, err
		}
		if lockByte == nil || len(lockByte) == 0 {
			if w := mp[string(key)]; w != nil {
				continue
			} else {
				server.DeleteLock(key, req.Context)
				server.DeleteVal(key, req.StartVersion, req.Context)
				server.RollBack(mvcc.EncodeKey(key, req.StartVersion), req.StartVersion, req.Context)
				continue
			}
		}
		lock, err := mvcc.ParseLock(lockByte)
		if err != nil {
			return nil, err
		}
		server.DeleteVal(key, req.StartVersion, req.Context)
		server.RollBack(mvcc.EncodeKey(key, req.StartVersion), req.StartVersion, req.Context)
		if lock.Ts == req.StartVersion {
			server.DeleteLock(key, req.Context)
		}

	}
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(engine_util.CfLock)
	keys := make([][]byte, 0)
	for iter.Valid() {
		key := iter.Item().Key()
		val, err := iter.Item().Value()
		if err != nil || len(val) == 0 {
			continue
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			continue
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, key)
		}
		iter.Next()
	}
	if req.CommitVersion == 0 {
		server.Latches.AcquireLatches(keys)
		defer server.Latches.ReleaseLatches(keys)
		for _, key := range keys {
			server.RollBack(mvcc.EncodeKey(key, req.StartVersion), req.StartVersion, req.Context)
			server.DeleteVal(key, req.StartVersion, req.Context)
			server.DeleteLock(key, req.Context)
		}
	} else {
		resp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.ResolveLockResponse{
			Error: resp.Error,
		}, nil
	}
	return &kvrpcpb.ResolveLockResponse{}, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
