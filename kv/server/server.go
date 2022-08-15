package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
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
		if regionErr, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version) // ver
	defer txn.Reader.Close()
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		// already locked & parselock err
		if regionErr, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	if lock == nil || lock.Ts > req.Version { // ttl
		value, err := txn.GetValue(req.Key)
		if err != nil {
			if regionErr, have := err.(*raft_storage.RegionError); have {
				resp.RegionError = regionErr.RequestErr
			}
			return resp, nil
		}
		if value == nil {
			resp.NotFound = true
		}
		resp.Value = value
		return resp, nil
	}
	resp.Error = &kvrpcpb.KeyError{
		Locked: lock.Info(req.Key),
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if errRegion, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = errRegion.RequestErr
			return resp, nil
		}
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	defer txn.Reader.Close()
	checkErr := make([]*kvrpcpb.KeyError, 0)
	for i := range req.Mutations {
		operator := req.Mutations[i]
		write, commitTs, err := txn.MostRecentWrite(operator.Key)
		if err != nil {
			if errRegion, have := err.(*raft_storage.RegionError); have {
				resp.RegionError = errRegion.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if write != nil && txn.StartTS <= commitTs {
			checkErr = append(checkErr, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTs,
					Key:        operator.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		lock, err := txn.GetLock(operator.Key)
		if err != nil {
			if errRegion, have := err.(*raft_storage.RegionError); have {
				resp.RegionError = errRegion.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			checkErr = append(checkErr, &kvrpcpb.KeyError{
				Locked: lock.Info(operator.Key),
			})
			continue
		}
		applyLock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
		}
		switch operator.Op {
		case kvrpcpb.Op_Del:
			applyLock.Kind = mvcc.WriteKindDelete
			txn.DeleteValue(operator.Key) //
			txn.PutLock(operator.Key, applyLock)
		case kvrpcpb.Op_Put:
			applyLock.Kind = mvcc.WriteKindPut
			txn.PutValue(operator.Key, operator.Value)
			txn.PutLock(operator.Key, applyLock)
		}
	}
	if len(checkErr) != 0 {
		resp.Errors = checkErr
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if errRegion, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = errRegion.RequestErr
			return resp, nil
		}
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// 110 nil [3] 100
	log.Infof("[DEBUG]  %v %v %v %v", req.CommitVersion, req.Context, req.Keys, req.StartVersion)
	resp := &kvrpcpb.CommitResponse{}
	if req.CommitVersion < req.StartVersion {
		resp.Error = &kvrpcpb.KeyError{
			Abort: "Abort",
		}
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if errRegion, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = errRegion.RequestErr
			return resp, nil
		}
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	defer txn.Reader.Close()
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for i := range req.Keys {
		key := req.Keys[i]
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			if errRegion, have := err.(*raft_storage.RegionError); have {
				resp.RegionError = errRegion.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback && commitTs == req.CommitVersion && req.StartVersion == write.StartTS {
			// duplicate KvCommit
			log.Infof("[duplicate] %v %v %v", commitTs, req.CommitVersion, req.StartVersion)
			return resp, nil
		}
		lock, err := txn.GetLock(key)
		log.Infof("[GetLock Commit] %v %v %v", key, lock, err)
		if err != nil {
			if errRegion, have := err.(*raft_storage.RegionError); have {
				resp.RegionError = errRegion.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if lock == nil {
			resp.Error = &kvrpcpb.KeyError{
				Locked: nil,
			}
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "Retryable",
			}
			return resp, nil
		}
		log.Infof("[GetLock] %v %v %v", lock.Kind, lock.Ts, lock.Primary)
		// if lock != nil && lock.Ts == txn.StartTS {
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
		// continue
		// }
	}
	// for i := range req.Keys {
	// 	key := req.Keys[i]
	// 	lock, _ := txn.GetLock(key)
	// 	txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
	// 		StartTS: req.StartVersion,
	// 		Kind:    lock.Kind,
	// 	})
	// 	txn.DeleteLock(key)
	// }
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if errRegion, have := err.(*raft_storage.RegionError); have {
			resp.RegionError = errRegion.RequestErr
			return resp, nil
		}
		return resp, err
	}
	return resp, nil
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
