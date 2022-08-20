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
	"sort"

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
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	ents, _ := storage.Entries(firstIndex, lastIndex+1)
	raftLog := &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         ents,
		pendingSnapshot: nil,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstindex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	if firstindex > l.FirstIndex() {
		if len(l.entries) > 0 {
			if firstindex > l.LastIndex() {
				l.entries = nil
			} else {
				l.entries = l.entries[l.entIdx2slcIdx(firstindex):]
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
	return l.entries[l.entIdx2slcIdx(l.stabled)+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 || l.committed == l.applied {
		return nil
	}
	return l.entries[l.entIdx2slcIdx(l.applied)+1 : l.entIdx2slcIdx(l.committed)+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	lastindex, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return lastindex
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	length := uint64(len(l.entries))
	if length > 0 && i-l.FirstIndex() >= 0 && i-l.FirstIndex() < length {
		return l.entries[l.entIdx2slcIdx(i)].Term, nil
	}
	term, err := l.storage.Term(i)
	if err != nil && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	return term, err
}

// return entries with index [left, lastindex].
// if left > lastindex return nil
func (l *RaftLog) getEntries(left uint64) ([]pb.Entry, error) {
	if left < l.FirstIndex() {
		return nil, ErrUnavailable
	}
	lastIndex := l.LastIndex()
	if left > lastIndex || lastIndex == 0 {
		return nil, nil
	}
	return l.entries[l.entIdx2slcIdx(left):], nil
}

// append entries to raftlog
func (l *RaftLog) append(ents []*pb.Entry) {
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
}

// check to append entries
func (l *RaftLog) appendEntries(prevLogTerm uint64, prevLogIndex uint64, leaderCommit uint64, ents []*pb.Entry) (uint64, bool) {
	t, _ := l.Term(prevLogIndex)
	// quick go back in raft paper 5.3
	if t != prevLogTerm {
		index := min(prevLogIndex, l.LastIndex())
		for index > l.committed {
			term, err := l.Term(index)
			if err != nil || term != t {
				break
			}
			index--
		}
		return index, false
	}
	// find the largest inconflict index
	if len(l.entries) == 0 {
		l.append(ents)
	} else {
		for i, ent := range ents {
			t, err := l.Term(ent.Index)
			if err != nil || t != ent.Term {
				l.stabled = min(l.stabled, ent.Index-1)
				l.entries = l.entries[:l.entIdx2slcIdx(ent.Index-1)+1]
				l.append(ents[i:])
				break
			}
		}
	}

	if leaderCommit > l.committed {
		l.committed = min(leaderCommit, prevLogIndex+uint64(len(ents)))
	}
	return l.LastIndex(), true
}

// update commit index.
// call by leader to update commit.
// return true if commit update
func (l *RaftLog) updateCommit(term uint64, prs map[uint64]*Progress) bool {
	matches := make(uint64Slice, 0, len(prs))
	for _, pr := range prs {
		matches = append(matches, pr.Match)
	}
	sort.Sort(matches)
	//if len(prs) == 1......
	index := matches[(len(prs)-1)/2]
	if index > l.committed && l.entries[l.entIdx2slcIdx(index)].Term == term {
		l.committed = index
		return true
	}
	return false
}

func (l *RaftLog) handleSnapshot(snap *pb.Snapshot) {
	snapIndex := snap.Metadata.Index
	l.committed = snapIndex
	//l.stabled = snapIndex
	//l.applied = snapIndex
	l.entries = nil
	l.pendingSnapshot = snap
}

func (l *RaftLog) advanceSnapshot(sindex uint64) {
	l.stabled = max(l.stabled, sindex)
	l.applied = max(l.applied, sindex)
	if !IsEmptySnap(l.pendingSnapshot) && l.pendingSnapshot.Metadata.Index == sindex {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) entIdx2slcIdx(i uint64) int {
	return int(i - l.FirstIndex())
}

func (l *RaftLog) slcIdx2entIdx(i int) uint64 {
	return uint64(i) + l.FirstIndex()
}
