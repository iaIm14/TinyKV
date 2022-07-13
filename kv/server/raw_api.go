package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
// 调用链：RawGetRequest作为参数传入Server.RawGet方法，server 使用sto age.Reader方法(已经实现)解析req.Context接口得到StorageReader struct
// 然后用GetCF IterCF Close() 操作完成Get数据读取请求，返回一个[]bytes 类型的value 表示请求的Key对应的值value。使用value构造一个RawGetResponse()
//
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	if value == nil {
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
// 表层的RawAPI 使用Server==StandAloneServer 的Write方法更新db，使用WriteBatch构造Modify[]数组
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
		return &kvrpcpb.RawPutResponse{}, err
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
		return &kvrpcpb.RawDeleteResponse{}, err
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
		return &kvrpcpb.RawScanResponse{}, err
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
		value, _ := iter.Item().Value()
		ret.Kvs = append(ret.Kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
		iter.Next()
	}
	return ret, nil
}
