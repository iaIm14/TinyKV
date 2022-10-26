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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage, applied uint64) *RaftLog {
	raftLog := RaftLog{}
	state, _, _ := storage.InitialState()
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Fatal(err.Error())
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Fatal(err.Error())
	}
	raftLog.stabled = lastIndex
	raftLog.committed = state.Commit
	raftLog.applied = min(firstIndex-1, applied)
	raftLog.storage = storage
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	raftLog.entries = entries
	if err != nil {
		log.Fatal(err.Error())
	}
	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// l.storage.Snapshot()
	firstIndex, _ := l.storage.FirstIndex()
	if len(l.entries) == 0 {
		return
	}
	index := l.entries[0].Index
	if firstIndex > index {
		l.entries = l.entries[firstIndex-index:]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	if l.stabled > l.LastIndex() {
		panic("unstableEntries")
	}
	firstIndex := l.entries[0].Index
	if l.stabled < firstIndex-1 {
		return l.entries
	}
	return l.entries[l.stabled-firstIndex+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// if the entries is null
	if len(l.entries) == 0 {
		return nil
	}
	firstIndex := l.entries[0].Index
	if l.committed+1 < firstIndex {
		panic("nextEnts error!")
	}
	if l.applied+1 < firstIndex {
		panic(errors.New("nextEnts l.applied < firstIndex-1"))
	}
	if l.applied > l.committed {
		panic("nextEnts error!")
	}
	ents = l.entries[l.applied+1-firstIndex : l.committed+1-firstIndex]
	return
}

// FirstIndex == raftlog.entries[0].index or 0
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		firstIndex, _ := l.storage.FirstIndex()
		return firstIndex
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
	if !IsEmptySnap(l.pendingSnapshot) {
		pendingSnapshotIndex = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		raftlogIndex = l.entries[len(l.entries)-1].Index
	}
	storageIndex, _ = l.storage.LastIndex()
	if raftlogIndex != 0 {
		// log.Info("lastindex return raftlogIndex")
		return raftlogIndex
	} else if pendingSnapshotIndex != 0 {
		// log.Info("lastindex return pendingSnapshotIndex")
		return pendingSnapshotIndex
	} else if storageIndex != 0 {
		// log.Info("lastindex return storageIndex")
		return storageIndex
	} else {
		log.Info("WARN: lastindex return 0")
		return 0
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	// in Raftlog.entries
	if len(l.entries) > 0 && i >= l.entries[0].Index && i <= l.entries[len(l.entries)-1].Index {
		return l.entries[l.entIdx2slcIdx(i)].Term, nil
	}
	if !IsEmptySnap(l.pendingSnapshot) && l.pendingSnapshot.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	// i not in Raftlog.entries, should search in storage
	// if >lastIndex or <FirstIndex() will return 0,ErrCompacted or ErrUnavailable (MemoryStorage)
	// to pass test, should let it return
	return l.storage.Term(i)
}

func (l RaftLog) getEntries(index uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	if index < l.entries[0].Index || index > l.entries[len(l.entries)-1].Index {
		return nil
	}
	return l.entries[index-l.entries[0].Index:]
}

// LastTerm return last term
func (l *RaftLog) LastTerm() (uint64, error) {
	lastIndex := l.LastIndex()
	return l.Term(lastIndex)
}

func (l *RaftLog) entIdx2slcIdx(i uint64) int {
	return int(i - l.FirstIndex())
}
