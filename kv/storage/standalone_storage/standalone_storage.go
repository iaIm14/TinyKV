package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Conf    *config.Config
	engines *engine_util.Engines
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	if conf != nil {
		kvPath := path.Join(conf.DBPath, "KV")
		raftPath := path.Join(conf.DBPath, "Raft")
		ret := &StandAloneStorage{
			Conf: conf,
			engines: engine_util.NewEngines(
				engine_util.CreateDB(kvPath, false),
				nil,
				kvPath,
				raftPath,
			),
		}
		return ret
	}
	// Your Code Here (1).
	return &StandAloneStorage{}
}

func (s *StandAloneStorage) Start() error {
	//Your Code Here (1).
	return nil
}
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	ret, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return ret, nil
}
func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
	//	left cf_iterator.go debug
}
func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

// Reader use badger.Txn only
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
	//return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			wb.SetCF(v.Cf(), v.Key(), v.Value())
		case storage.Delete:
			wb.DeleteCF(v.Cf(), v.Key())
		}
	}
	return s.engines.WriteKV(wb)
}
