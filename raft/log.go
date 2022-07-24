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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
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
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// first entry in Raftlog.entries 's Index
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	ret := &RaftLog{
		storage: storage,
	}
	firstIndex, err := ret.storage.FirstIndex()
	if err != nil {
		return nil
	}
	lastIndex, err := ret.storage.LastIndex()
	if err != nil {
		return nil
	}
	entries, err := ret.storage.Entries(firstIndex, lastIndex+1) // note
	if err != nil {
		return nil
	}
	ret.entries = entries
	// left note
	ret.committed = firstIndex - 1
	ret.applied = firstIndex - 1
	ret.stabled = lastIndex
	ret.offset = firstIndex
	return ret
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {

	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled >= l.LastIndex() {
		return []pb.Entry{}
	}
	return l.getEntries(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied >= l.LastIndex() {
		return []pb.Entry{}
	}
	return l.getEntries(l.applied+1, l.committed+1)
}

// getEntries return all Entry Index between [lo,ro)
func (l *RaftLog) getEntries(lo, ro uint64) (ents []pb.Entry) {
	lastIndex := l.LastIndex()
	// note ro > lastIndex +1
	if lo > lastIndex || ro > lastIndex+1 || lo == ro {
		return []pb.Entry{}
	}
	if len(l.entries) > 0 {
		var ents []pb.Entry
		if lo < l.offset {
			entries, err := l.storage.Entries(lo, min(l.offset, ro))
			if err != nil {
				return nil
			}
			ents = entries
			log.Printf("[DEBUG] DEBUG getEntries lo<offset:%v", ents)
		}
		if ro > l.offset {
			ents = append(ents, l.entries[max(lo, l.offset)-l.offset:ro-l.offset]...)
			log.Printf("[DEBUG] DEBUG getEntries ro>offset:%v", ents)
		}
		log.Printf("[DEBUG] DEBUG getEntries: offset==%v lastIndex==%v", l.offset, l.LastIndex())
		return ents
	} else {
		ents, _ := l.storage.Entries(lo, ro)
		return ents
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		ret, _ := l.storage.LastIndex()
		return ret
	} else {
		return uint64(len(l.entries)) + l.offset - 1
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}
	return l.storage.Term(i)
}

// LastTerm return last term
func (l *RaftLog) LastTerm() (uint64, error) {
	lastIndex := l.LastIndex()
	return l.Term(lastIndex)
}

// BaseAppendEntries Follower append entries, maybe conflict
// base on etcd: func (*unstable) truncateAndAppend()
func (l *RaftLog) BaseAppendEntries(entries ...*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	prevIndex := entries[0].Index - 1
	switch {
	case prevIndex == l.offset+uint64(len(l.entries))-1:
		for _, v := range entries {
			l.entries = append(l.entries, *v)
		}
	case prevIndex < l.offset:
		l.stabled = min(l.stabled, prevIndex)
		l.offset = prevIndex + 1
		l.entries = []pb.Entry{}
		for _, v := range entries {
			l.entries = append(l.entries, *v)
		}
	default:
		l.stabled = min(prevIndex, l.stabled)
		l.entries = append([]pb.Entry{}, l.getEntries(l.offset, prevIndex+1)...)
		for _, v := range entries {
			l.entries = append(l.entries, *v)
		}
	}
}
