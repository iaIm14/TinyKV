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
	Status  bool
	engines *engine_util.Engines
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	if conf != nil {
		kvPath := path.Join(conf.DBPath, "KV")
		raftPath := path.Join(conf.DBPath, "Raft")
		ret := &StandAloneStorage{
			Conf:   conf,
			Status: false,
			engines: engine_util.NewEngines(
				engine_util.CreateDB(kvPath, false),
				// badger.DB.Open(badger.DefaultOptions(conf.DBPath)),
				engine_util.CreateDB(raftPath, true),
				kvPath,
				raftPath,
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

// GetCF 调用engine_util 工具函数 从StandAloneReader.Txn返回一个符合Txn事务查询的值
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
// 使用DB.NewTransaction(false) 创建只读事务txn 并且调用NewStandAloneReader返回一个StorageReader==StandAloneReader，
// 实现了GetCF&IterCF&Close三个接口
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	_ = ctx
	// key:= left
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
	//return nil, nil
}

// Write 还要使用util实现的功能函数
// Note 1
// 接收Modify struct 的数组存储多个不同操作，使用badger.DB.Newtransaction创建一个新的读写事务txn，
// 遍历整个batch数组的每一个元素，取得Key&Value&CF ，使用这三个新建一个Entry并且使用SetEntry设置到txn中,
// fault: 使用badger.txn建立调用NewStandAloneStorage 新建了StandAloneStorage，调用Write方法。
// 使用engine_util 中实现的具体接口更新StandAloneStorage.engines.kv(badger.DB)(由于要添加CF 支持，不需要直接调用DB.Update Set 等操作)
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	_ = ctx
	// Your Code Here (1).
	//txn := s.Engines.Kv.NewTransaction(true) // write
	//for _, v := range batch {
	//	key := batch.Key()
	//	val := batch.Value()
	//	cf := batch.Cf()
	//
	//	err := txn.SetEntry(key, val)
	//	if err != nil {
	//		return err
	//	}
	//}
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.engines.Kv, v.Cf(), v.Key(), v.Value())
		case storage.Delete:
			engine_util.DeleteCF(s.engines.Kv, v.Cf(), v.Key())
		}
	}
	//txn.Discard() // debug
	return nil
}
