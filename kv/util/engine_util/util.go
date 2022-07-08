package engine_util

// finish. left Raft
import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

// KeyWithCF Badger实现的LSM-Tree key &value部分是分开的
// KeyWithCF 给WriteBatch的Write&DeleteCF更新key，构造一个CF_key格式的新key返回
func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

// GetCF 获得CF对应的所有value值的数组
func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	// db.view 封装db.NewTransaction
	err = db.View(func(txn *badger.Txn) error {
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})
	return
}

// GetCFFromTxn badger.Txn 表示对象事务transaction，实现了Get(key []bytes),Delete(keys),
// Commit()读操作直接返回，写操作如果写入对象在事务Txn启动SetEntry(badger.Entry)之后更新了
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	// item返回的对象集合的链表上的节点，ValueCopy将其从存储value的WAL拷贝到val
	// ，实际上调用了package y中的SafeCopy函数
	val, err = item.ValueCopy(val)
	return
}

func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
	//  db.update封装 txn.commit()&txn.discard()
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func DeleteCF(engine *badger.DB, cf string, key []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
