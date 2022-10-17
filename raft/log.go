// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	PendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	ret := &RaftLog{
		storage: storage,
	}

	// possibly available
	firstIndex, _ := ret.storage.FirstIndex()
	lastIndex, _ := ret.storage.LastIndex()
	entries, _ := ret.storage.Entries(firstIndex, lastIndex+1) // note
	ret.entries = entries
	// left note
	// raft Node restart
	// debug note : nothing left but a snapshot , entries.empty()== true : is legal.
	ret.committed = firstIndex - 1
	ret.applied = firstIndex - 1
	ret.stabled = lastIndex
	// 当前第一条还没有被合并到快照中的LogIndex
	// 更新时机: RaftStorage 写入的时候
	// ret.FirstIndex = firstIndex
	ret.PendingSnapshot = nil
	return ret
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// MemoryStorage.entries[0].Index+1==FirstIndex
	// dummy entries
	// TODO: when write the Raftlog into Storage and what situation might occur?
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		log.Error("maybeCompact call storage.fristIndex return err: ", err)
		panic(err)
	}
	// RaftLog.entries contains [maybeFirstIndex==latest Snapshot's lastindex+1,lastindex]
	// a new Snapshot will update actual fristIndex , at the same time l.entries still have some scale entries.
	// maybe cause [ERROR: slice out of range].
	// if firstIndex > l.FirstIndex {
	// 	if len(l.entries) != 0 {
	// 		entries := l.entries[firstIndex-l.FirstIndex:]
	// 		l.entries = entries
	// 	}
	// 	l.FirstIndex = firstIndex
	// }
	if firstIndex > l.FirstIndex() {
		// TODO : is it possible? when node restart , might append a noop entry.
		if len(l.entries) == 0 {
			// log.Info("WARN: RaftLog have no entries")
			// l.FirstIndex = firstIndex
			// return
		} else {
			if firstIndex > l.entries[len(l.entries)-1].Index {
				l.entries = []pb.Entry{}
				return
			} else {
				l.entries = l.entries[firstIndex-l.FirstIndex():]
				return
			}
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	// NOTE: Need Fix. stabled will update in ready(RawNode module), not in Raft module
	// if l.stabled < l.committed {
	// 	log.Error("[ERROR] unstableEntries l.stabled < l.committed.")
	// 	panic(errors.New("unstableEntries l.stabled < l.committed"))
	// }
	// if l.stabled < l.FirstIndex() {
	// 	log.Error("[ERROR] unstableEntries l.stabled < l.FirstIndex().")
	// 	panic(errors.New("unstableEntries l.stabled < l.FirstIndex()"))
	// }
	if l.stabled > l.LastIndex() {
		log.Error("[ERROR] unstableEntries l.stabled >=l.lastIndex.")
		panic(errors.New("unstableEntries l.stabled >=l.lastIndex"))
		// return []pb.Entry{}
	}
	return l.entries[l.stabled+1-l.FirstIndex():]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied > l.committed {
		log.Error("[ERROR] nextEnts l.applied > l.committed.")
		panic(errors.New("nextEnts l.applied > l.committed"))
	}
	if len(l.entries) == 0 {
		return nil
		// debug info : return []pb.Entry{}
	}
	return l.entries[l.applied+1-l.FirstIndex() : l.committed+1-l.FirstIndex()]
}

// getEntries return all Entry Index between [lo,ro)
func (l *RaftLog) getEntries(lo, ro uint64) (ents []pb.Entry) {
	return l.entries[lo-l.FirstIndex() : ro-l.FirstIndex()]
}

//  FirstIndex == raftlog.entries[0].index or 0
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// stale snapshot discarded. debuginfo
	var pendingSnapshotIndex uint64 = 0
	var raftlogIndex uint64 = 0
	var storageIndex uint64 = 0
	if !IsEmptySnap(l.PendingSnapshot) {
		pendingSnapshotIndex = l.PendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		raftlogIndex = l.entries[len(l.entries)-1].Index
	}
	storageIndex, err := l.storage.LastIndex()
	if err != nil {
		storageIndex = 0
	}
	if raftlogIndex != 0 {
		log.Info("lastindex return raftlogIndex")
		return raftlogIndex
	} else if pendingSnapshotIndex != 0 {
		log.Info("lastindex return pendingSnapshotIndex")
		return pendingSnapshotIndex
	} else if storageIndex != 0 {
		log.Info("lastindex return storageIndex")
		return storageIndex
	} else {
		log.Info("WARN: lastindex return 0")
		return 0
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.FirstIndex() {
		if i > l.entries[len(l.entries)-1].Index {
			log.Info("[ERROR] Term() i>=raftlog.entries[last].index")
		} else {
			return l.entries[i-l.FirstIndex()].Term, nil
		}
	}
	// i not in Raftlog.entries, should search in storage
	// if >lastIndex or <FirstIndex() will return 0,ErrUnavailable(MemoryStorage)
	// to pass test, should let it return
	term, err := l.storage.Term(i)

	if err != nil && !IsEmptySnap(l.PendingSnapshot) {
		if err != ErrUnavailable {
			log.Info("[WARN] storage.Term return err but not ErrUnavailable")
		}
		if i == l.PendingSnapshot.Metadata.Index {
			term = l.PendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.PendingSnapshot.Metadata.Index {
			err = ErrCompacted
		} else {
			log.Error("storage.Term return ErrUnavailable but i.index > snapshot's Index")
			panic(errors.New("storage.Term return ErrUnavailable but i.index > snapshot's Index"))
		}
	}
	return term, err
}

// LastTerm return last term
func (l *RaftLog) LastTerm() (uint64, error) {
	lastIndex := l.LastIndex()
	return l.Term(lastIndex)
}
