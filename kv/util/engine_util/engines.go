package engine_util

import (
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
)

// finish

// Engines keeps references to and data for the engines used by unistore.
// All engines are badger key/value databases.
// the Path fields are the filesystem path to where the data is stored.
// Engines 存储KV数据库的存储文件地址使用的
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	// badger 实现的LSM-Tree数据库，使用key-value分离的方式，在key大小远小于value的时候性能更好
	Kv     *badger.DB
	KvPath string
	// Metadata used by Raft.
	Raft     *badger.DB
	RaftPath string
}

// NewEngines 新建 Engines
func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv:       kvEngine,
		KvPath:   kvPath,
		Raft:     raftEngine,
		RaftPath: raftPath,
	}
}

// WriteKV 使用WriteBatch.WriteToDB 写Engines.Kv
func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

// WriteRaft 使用WriteBatch.WriteToDB 写Engines.RaftKv
func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

// Close 关闭Engine的两个KV数据库实现
func (en *Engines) Close() error {
	if err := en.Kv.Close(); err != nil {
		return err
	}
	if err := en.Raft.Close(); err != nil {
		return err
	}
	return nil
}

// Destroy 关闭数据库并且释放两个数据库
func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB creates a new Badger DB on disk at path.
func CreateDB(path string, raft bool) *badger.DB {
	opts := badger.DefaultOptions
	if raft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
	}
	opts.Dir = path
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
