package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	searchKey []byte
	txn       *MvccTxn
	iter      engine_util.DBIterator
	// Your Data Here (4C).
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := &Scanner{
		searchKey: startKey,
		txn:       txn,
		iter:      iter,
	}
	return scanner
}

func (scan *Scanner) Close() {
	scan.iter.Close()
	// Your Code Here (4C).
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.searchKey == nil {
		return nil, nil, nil
	}
	if !scan.iter.Valid() {
		scan.searchKey = nil
		return nil, nil, nil
	}
	searchTs := scan.txn.StartTS
	findUserKey := scan.searchKey
	var findTs uint64
	var findItem engine_util.DBItem
	for {
		// if scan.iter.Valid() && findTs > searchTs {
		scan.iter.Seek(EncodeKey(findUserKey, searchTs))
		// Next smallest Key with searchTs
		if !scan.iter.Valid() {
			scan.searchKey = nil
			return nil, nil, nil
		}
		findTs = decodeTimestamp(scan.iter.Item().Key())
		findUserKey = DecodeUserKey(scan.iter.Item().Key())
		if findTs <= searchTs {
			// findUserKey
			findItem = scan.iter.Item()
			break
		}
	}
	if !scan.iter.Valid() {
		scan.searchKey = nil
		return nil, nil, nil
	}

	// update SearchKey: Next Key with different Userkey
	for ; scan.iter.Valid(); scan.iter.Next() {
		if !bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), findUserKey) {
			break
		}
	}
	if !scan.iter.Valid() {
		scan.searchKey = nil
	} else {
		scan.searchKey = DecodeUserKey(scan.iter.Item().Key())
	}

	writeVal, err := findItem.Value()
	if err != nil {
		return findUserKey, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return findUserKey, nil, err
	}
	if write.Kind == WriteKindRollback || write.Kind == WriteKindDelete {
		return findUserKey, nil, nil
	}
	ret, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(findUserKey, write.StartTS))
	return findUserKey, ret, err
}
