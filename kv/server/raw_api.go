package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err == nil && len(value) > 0 {
		return &kvrpcpb.RawGetResponse{
			Value:    value,
			NotFound: false,
		}, nil
	}
	if len(value) == 0 {
		return &kvrpcpb.RawGetResponse{NotFound: true, Value: nil}, nil
	}
	return nil, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := []storage.Modify{storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := []storage.Modify{storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: nil,
			Cf:    req.Cf,
		},
	}}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	resp := &kvrpcpb.RawScanResponse{}
	for iter.Valid() && req.Limit > 0 {
		req.Limit--
		kvp := kvrpcpb.KvPair{}
		kvp.Key = iter.Item().KeyCopy(kvp.Key)
		kvp.Value, err = iter.Item().ValueCopy(kvp.Value)
		if err != nil || iter.Item().ValueSize() == 0 {
			iter.Next()
			continue
		}
		resp.Kvs = append(resp.Kvs, &kvp)
		iter.Next()
	}
	return resp, nil
}
