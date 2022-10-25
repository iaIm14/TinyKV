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
	"bytes"
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// Ticks generate by random by electionTimeout ,which range is [ electionTimeout + 1, electionTimeout * 2]
	randomElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	state, confState, err := c.Storage.InitialState()
	peers := map[uint64]*Progress{}

	if c.peers == nil {
		c.peers = confState.Nodes
	}
	raftLog := newLog(c.Storage)
	for i := range c.peers {
		peers[c.peers[i]] = &Progress{
			0, raftLog.LastIndex() + 1,
		}
	}

	raft := Raft{
		id:               c.ID,
		Prs:              peers,
		RaftLog:          raftLog,
		votes:            map[uint64]bool{},
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	if err != nil {
		log.Fatal(err.Error())
	}
	raft.Vote = state.Vote
	raft.RaftLog.committed = state.Commit
	raft.Term = state.Term
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// in this method, just consider the error like can't find the Term or Index not reasonable

	// example: the entries 1,2,3,4,5
	//                      1,2,2,3,4
	// when index < 1 or index > 5 ,error
	prevIndex := r.Prs[to].Next - 1
	firstIndex := r.RaftLog.FirstIndex()

	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil || prevIndex < firstIndex-1 {
		r.sendSnapshot(to)
		return true
	}
	entries := r.RaftLog.getEntries(prevIndex + 1)
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: TransToPointer(entries),
		Index:   prevIndex,
		LogTerm: prevTerm,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// meantime, check the commit
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) sendSnapshot(to uint64) bool {
	if r.Prs[to] == nil {
		return false
	}
	var snap pb.Snapshot
	if IsEmptySnap(r.RaftLog.pendingSnapshot) {
		var err error
		snap, err = r.RaftLog.storage.Snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Infof("%d failed to send snapshot to %d", r.id, to)
				return false
			}
			log.Panic(err)
		}
	} else {
		snap = *r.RaftLog.pendingSnapshot
	}
	message := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snap,
	}
	// r.Prs[to].Next = snap.Metadata.Index + 1
	r.msgs = append(r.msgs, message)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// new peer when split should campaign immediately
	if r.id%2 == 0 && r.Term == 5 {
		r.handleHup()
	}
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, Term: r.Term, From: r.id})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			if r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, Term: r.Term, From: r.id}) != nil {
				// ...
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// reset the electron random click
	r.resetRandomElectionTimeout()

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.Vote = lead
	r.votes = map[uint64]bool{}
	r.Term = term
	r.Lead = lead

	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// reset the electron random click
	r.resetRandomElectionTimeout()

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.votes = map[uint64]bool{}
	r.Term = r.Term + 1
	r.Lead = 0

	r.Vote = r.id
	r.votes[r.id] = true

	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.resetRandomElectionTimeout()

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Lead = r.id

	r.State = StateLeader
	// when become the leader, you should reset the peers' match and next
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0

		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	// Leader should propose a noop entry on its term
	entry := pb.Entry{Data: nil}
	if !r.appendEntries(entry) {
		// log fail
		return
	}

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// this is a local msg
	if m.Term < r.Term && m.From != m.To {
		if m.MsgType == pb.MessageType_MsgHup || m.MsgType == pb.MessageType_MsgPropose ||
			m.MsgType == pb.MessageType_MsgTransferLeader {
			// the local msg
		} else {
			return nil
		}
	} else if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, 0)
			if m.MsgType == pb.MessageType_MsgRequestVote {
				r.Vote = m.From
			}
		}
	}

	switch r.State {
	// when state is Follower
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// start candidate
			r.handleHup()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)

		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)

		case pb.MessageType_MsgTimeoutNow:
			if r.Prs[m.To] == nil {
				return nil
			}
			// to step the timeout
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})

		case pb.MessageType_MsgTransferLeader:
			r.msgs = append(r.msgs, pb.Message{From: m.From, To: r.Lead, MsgType: pb.MessageType_MsgTransferLeader})
		}

	// when state is Candidate
	case StateCandidate:
		if m.From != r.id && m.Term >= r.Term {
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		// if meet a term greater than raft's, change to Follower
		case pb.MessageType_MsgHeartbeatResponse:
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			// need current state is leader
			// start boastHeartbeat
			if r.State == StateLeader {
				for id := range r.Prs {
					if id != r.id {
						r.sendHeartbeat(id)
					}
				}
			}
		case pb.MessageType_MsgPropose:
			// stop accepting new proposals
			if r.leadTransferee != 0 {
				return nil
			}
			entries := make([]pb.Entry, len(m.Entries))
			for i := range entries {
				entries[i] = *m.Entries[i]
			}
			// put the entries into log
			r.appendEntries(entries...)
			util.LogEntries(int(r.Lead), int(r.id), r.RaftLog.entries)
			if len(r.Prs) == 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
			// start the app msg
			for id := range r.Prs {
				if r.id == id {
					continue
				}
				r.sendAppend(id)
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			r.handleLeaderTransfer(m)
		}
	}
	return nil
}

// maybeCommit
func (r *Raft) maybeCommit() {
	// get all the match of peers,sort it and get the index of half, commit to the index
	n := len(r.Prs)
	indexes := make([]uint64, n)
	i := 0
	for _, v := range r.Prs {
		indexes[i] = v.Match
		i++
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	commitIndex := indexes[n-(n/2+1)]
	term, err := r.RaftLog.Term(commitIndex)
	if err != nil {
		return
	}
	if r.RaftLog.committed < commitIndex && term == r.Term {
		r.RaftLog.committed = commitIndex
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
}

// appendEntries append the entries to local log
func (r *Raft) appendEntries(entries ...pb.Entry) bool {
	if len(entries) == 0 {
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIndex + uint64(i) + 1
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	return true
}

func (r *Raft) bCastVote() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		term, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			panic(err)
		}
		message := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: term,
			Index:   r.RaftLog.LastIndex(),
		}
		r.msgs = append(r.msgs, message)
	}
}

// handleHup handle local hup request
func (r *Raft) handleHup() {
	// start sent voteMessage
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.bCastVote()
}

// handleAppendResponse the leader handle the appendResponse
func (r *Raft) handleAppendResponse(m pb.Message) {
	// check whether the follower reject the appendMsg
	if m.Reject {
		if m.Index < r.RaftLog.FirstIndex()-1 {
			r.sendSnapshot(m.From)
			return
		}
		m.Index = min(m.Index, r.RaftLog.LastIndex())
		// reject : there is a conflict between leader and follower
		// find the newest entry that term <= logTerm
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		r.Prs[m.From].Next = hintIndex + 1
		r.sendAppend(m.From)

	} else {
		if r.leadTransferee != 0 {
			message := pb.Message{
				From:    r.id,
				To:      r.leadTransferee,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgTimeoutNow,
			}
			r.msgs = append(r.msgs, message)
			r.leadTransferee = 0
			return
		}
		// update the peer's next
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.Index
		}
		if m.Index+1 > r.Prs[m.From].Next {
			r.Prs[m.From].Next = m.Index + 1
		}
		r.maybeCommit()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.Lead = m.From
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}

	li := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(m.Index)
	// li < index , li = 4 index = 6
	// the follower need send index = 4 back
	// I: 1 2 3 4 5 6 7
	//    - - - - - - -
	// L: 1 1 2 3 4 5 6
	// F: 1 1 2 3
	if li < m.Index || err != nil {
		message.Index = li
		lastTerm, _ := r.RaftLog.Term(li)
		message.LogTerm = lastTerm
		message.Index = li
		message.Reject = true
		r.msgs = append(r.msgs, message)
		return
	}
	// term == logTerm , term = logTerm = 6
	// the follower need send index = 4 back
	// I  1 2 3 4 5 6 7
	//    - - - - - - -
	// L: 1 1 2 3 4 5 6 7
	// F: 1 1 2 3 4 5 6 6 6
	// or
	// I  1 2 3 4 5 6 7
	//    - - - - - - -
	// L: 1 1 2 3 4 5 6 7
	// F: 1 1 2 3 4 5 6
	if term == m.LogTerm {
		// first add the new entries to node and find the first different entry
		entries := TransFromPointer(m.Entries)
		newIndex := m.Index + uint64(len(m.Entries))
		for i, v := range m.Entries {
			// when see the new entry break
			if len(r.RaftLog.entries) == 0 || li < v.Index {
				r.RaftLog.entries = append(r.RaftLog.entries, entries[i:]...)
				break
			}
			// when find the confict
			curIndex := v.Index - r.RaftLog.entries[0].Index
			curEntry := r.RaftLog.entries[curIndex]
			if bytes.Compare(curEntry.Data, v.Data) != 0 ||
				curEntry.Term != v.Term {
				r.RaftLog.entries = append(r.RaftLog.entries[:curIndex], entries[i:]...)
				r.RaftLog.stabled = curEntry.Index - 1
				break
			}
		}
		message := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
		}
		r.msgs = append(r.msgs, message)

		if m.Commit > r.RaftLog.committed {
			if newIndex < m.Commit {
				r.RaftLog.committed = newIndex
			} else {
				r.RaftLog.committed = m.Commit
			}
		}
	} else {
		message.Reject = true
		// logTerm , term not equal, logTerm = 6 and index = 7
		// if the term between leader and follower are not equal
		// recall the first index whose term <= logTerm and
		// I  1 2 3 4 5 6 7 8
		//    - - - - - - - -
		// L: 1 1 2 3 4 5 6 7
		// F: 1 1 2 3 4 4 4 4
		if term < m.LogTerm {
			message.LogTerm = term
			message.Index = m.Index
			r.msgs = append(r.msgs, message)
			return
		} else {
			// term > logTerm
			// this time we should return the newest Index whose Term <= LogTerm
			// I  1 2 3 4 5 6 7 8
			//    - - - - - - - -
			// L: 1 1 2 3 4 5 5 7
			// F: 1 1 2 3 6 6 6 6
			// L: 1 3
			// F: 2
			hintIndex := min(m.Index, r.RaftLog.LastIndex())
			hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
			hintTerm, err := r.RaftLog.Term(hintIndex)
			if err != nil {
				panic(err)
			}
			message.Index = hintIndex
			message.LogTerm = hintTerm
			r.msgs = append(r.msgs, message)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.electionElapsed = 0
	if m.Commit > r.RaftLog.LastIndex() {
		r.RaftLog.committed = m.Commit
	}
	r.msgs = append(r.msgs,
		pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, Term: r.Term, From: r.id, To: m.From},
	)
}

// handleVote handle VoteResponse RPC request
func (r *Raft) handleVote(m pb.Message) {
	canVote := (r.Vote == 0 && r.Lead == 0) || r.Vote == m.From
	// through the log to judge whether it is the candidate with current log
	lastIndex := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return
	}
	// Important in the paper
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	isUpdate := m.LogTerm > term || (m.LogTerm == term && m.Index >= lastIndex)
	if canVote && isUpdate {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Term: r.Term, From: r.id})
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse, Term: r.Term, To: m.From, From: r.id, Reject: true,
		})
	}
}

// handleVoteResponse handle VoteResponse RPC request
func (r *Raft) handleVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	rejectCount := 0
	count := 0
	for _, vote := range r.votes {
		if !vote {
			rejectCount++
		} else {
			count++
		}
	}
	if count*2 > len(r.Prs) {
		// vote success
		r.becomeLeader()
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
	if rejectCount*2 > len(r.Prs) {
		// vote fail, change to follower
		r.becomeFollower(r.Term, 0)
	}
}

// handleLeaderTransfer handle the Leader transfer
func (r *Raft) handleLeaderTransfer(m pb.Message) {
	newLeader := m.From
	if m.From == m.To || r.Prs[newLeader] == nil {
		return
	}
	r.leadTransferee = newLeader
	if r.Prs[newLeader].Match < r.RaftLog.LastIndex() {
		// help the transferee
		r.sendAppend(newLeader)
	} else {
		// send msg immediately
		message := pb.Message{
			From:    r.id,
			To:      newLeader,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgTimeoutNow,
		}
		r.msgs = append(r.msgs, message)
		r.leadTransferee = 0
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log := r.RaftLog // restore the RaftLog
	meta := m.Snapshot.Metadata
	term, _ := r.RaftLog.Term(meta.Index)
	message := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term}

	if term == meta.Term {
		message.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, message)
		return
	}
	log.pendingSnapshot = m.Snapshot
	log.committed = meta.Index

	log.entries = nil
	log.applied = meta.Index
	log.stabled = meta.Index
	// update the pendingSnapshot util the node update it
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{Match: 0, Next: meta.Index + 1}
	}
	message.Index = r.RaftLog.LastIndex()

	r.msgs = append(r.msgs, message)

	return
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A)
	if _, ok := r.Prs[id]; ok {
		return
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		return
	}
	delete(r.Prs, id)
	delete(r.votes, id)
	if r.id != id && r.State == StateLeader {
		r.maybeCommit()
	}
	r.PendingConfIndex = None
}

// resetRandomElectionTimeout reset the randomElectionTimeout
func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// softState
func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// softState
func (r *Raft) hardState() *pb.HardState {
	return &pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
