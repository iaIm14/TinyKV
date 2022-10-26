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
func newLog(storage Storage) *RaftLog {
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
	raftLog.applied = firstIndex - 1
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

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if l.lenEntries() > 0 {
		return l.entries[0].Index + l.lenEntries() - 1
	} else {
		if !IsEmptySnap(l.pendingSnapshot) {
			return l.pendingSnapshot.Metadata.Index
		}
		// find it from snap
		lastIndex, _ := l.storage.LastIndex()
		return lastIndex
	}
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if l.lenEntries() > 0 {
		return l.entries[0].Index
	} else {
		firstIndex, _ := l.storage.FirstIndex()
		return firstIndex
	}
}

// lenEntries return the length of entries
func (l *RaftLog) lenEntries() uint64 {
	return uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) splitEntries(i uint64) (ents []pb.Entry) {
	if l.lenEntries() == 0 {
		return
	} else {
		index := i - l.entries[0].Index
		ents = l.entries[index:]
		return
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// fmt.Println(l.LastIndex())
	s := l.pendingSnapshot
	if s != nil && s.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if l.lenEntries() == 0 || i < l.FirstIndex() {
		return l.storage.Term(i)
	}
	return l.entries[i-l.FirstIndex()].Term, nil
}

func (l RaftLog) getEntries(index uint64) []pb.Entry {
	firstIndex := l.FirstIndex()
	lastIndex := l.LastIndex()
	if index < firstIndex || index > lastIndex {
		return nil
	}
	return l.entries[index-firstIndex:]
}

func (l RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}
