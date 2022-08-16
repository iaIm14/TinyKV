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
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.NotFound = true
		resp.Error = err.Error()
		return resp, nil
	}
	if len(value) == 0 {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// _ = req.Context
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp := &kvrpcpb.RawPutResponse{}
		resp.Error = err.Error()
		return resp, nil
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
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	if !iter.Valid() {
		return &kvrpcpb.RawScanResponse{}, nil
	}
	limit := req.Limit
	for v := uint32(0); v < limit; v++ {
		if !iter.Valid() {
			break
		}
		value, err := iter.Item().Value()
		if err != nil {
			continue
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
		iter.Next()
	}
	return resp, nil
}
