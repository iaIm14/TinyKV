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
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
			Error:    err.Error(),
		}, nil
	}
	if len(value) == 0 {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	ret := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}
	return ret, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// _ = req.Context
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		// left  v.Cf(), v.Key(),
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, err
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	ret := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, nil
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	if iter.Valid() == false {
		iter.Close()
		return &kvrpcpb.RawScanResponse{}, nil
	}
	limit := req.Limit
	for v := 1; v <= int(limit); v++ {
		if iter.Valid() == false {
			iter.Close()
			break
		}
		value, err := iter.Item().Value()
		if err != nil {
			iter.Close()
			break
		}
		ret.Kvs = append(ret.Kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
		iter.Next()
	}
	return ret, nil
}
