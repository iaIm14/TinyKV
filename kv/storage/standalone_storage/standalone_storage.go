package standalone_storage

import (
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
	Status  bool
	engines *engine_util.Engines
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	if conf != nil {
		ret := &StandAloneStorage{
			Conf:   conf,
			Status: false,
			engines: engine_util.NewEngines(
				engine_util.CreateDB(conf.DBPath, false),
				// badger.DB.Open(badger.DefaultOptions(conf.DBPath)),
				engine_util.CreateDB(conf.DBPath, true),
				conf.DBPath,
				conf.DBPath,
			),
			// debug
		}
		return ret
	}
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Start() error {
	//if(s.Conf.StoreAddr != ""&& s.Conf.SchedulerAddr!="")
	//if s.Conf.DBPath == "" {
	//	return
	//}
	//Your Code Here (1).
	s.Status = true
	return nil
}
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.Status = false
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
	_ = ctx
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
	//return nil, nil
}

// Write 还要使用util实现的功能函数
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	_ = ctx
	// Your Code Here (1).
	txn := s.engines.Kv.Newtransaction(true) // write
	key := batch.Key()
	val := batch.Value()
	cf := batch.Cf()
	err := txn.SetEntry(key, val)
	if err != nil {
		return err
	}
	switch batch.Data.(type) {
	case storage.Put:
		engine_util.PutCF(s.engines.Kv, cf, key, val)
	case storage.Delete:
		engine_util.DeleteCF(s.engines.Kv, cf, key)
	}
	txn.Discard() // debug
	return nil
}
