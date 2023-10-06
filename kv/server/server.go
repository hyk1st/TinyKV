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
	// 检查锁冲突
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && (lock.Ts != req.StartVersion && (lock.Ts+lock.Ttl >= req.StartVersion || lock.Ttl == 0)) {
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
			return resp, nil
		}
	}
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
			return resp, err
		}
	}

	// 检查锁的状态
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && ((lock.Ts != req.StartVersion && (lock.Ts+lock.Ttl >= req.StartVersion || lock.Ttl == 0)) || (lock.Ts == req.StartVersion && lock.Ts+lock.Ttl < req.StartVersion && lock.Ttl != 0)) {
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
		Locks := make([]storage.Modify, len(req.Keys))
		for _, key := range req.Keys {
			Locks = append(Locks, storage.Modify{Data: storage.Delete{
				Key: key,
				Cf:  engine_util.CfLock,
			}})
			txn.DeleteLock(key)
		}
		_ = server.storage.Write(req.Context, Locks)
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
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
