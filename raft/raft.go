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
	"fmt"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/gogo/protobuf/sortkeys"
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

type SnapStateType uint64

const (
	SnapStateNormal SnapStateType = iota
	SnapSending
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// [check debug] unused
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
	// [1,2,3,4][5] & 5 is leader, 1,2,3,4 will raiseElection& elect a new leader, which will cause
	// add checkQuorum
	recentAlive bool
	// SnapSending: shouldn't sendAppend or sendSnapshot
	// TODO: SnapshotTicker should change to SnapStatus
	SnapState      SnapStateType
	SnapLastIndex  uint64
	SnapshotTicker int
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
	electionTimeout       int
	electionRandomTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	leadTransfereeElapsed int
	// Communicate           map[uint64]int
	// haveSendedSnapShot    map[uint64]int
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
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage, c.Applied),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower, //
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	hardState, confState, _ := r.RaftLog.storage.InitialState()
	r.Term, r.Vote, r.RaftLog.committed = hardState.Term, hardState.Vote, hardState.Commit
	// Todo: might change regionID when recovery

	peers := c.peers
	if len(confState.Nodes) != 0 {
		peers = confState.Nodes
	}
	for _, i := range peers {
		if i == r.id {
			r.Prs[i] = &Progress{Match: lastIndex, Next: lastIndex + 1, recentAlive: true}
		} else {
			r.Prs[i] = &Progress{Match: 0, Next: lastIndex + 1, recentAlive: true}
		}
	}
	return r
}

func (r *Raft) sendSnapshot(to uint64) bool {
	if r.Prs[to] == nil || !r.Prs[to].recentAlive || r.Prs[to].SnapState == SnapSending {
		return false
	}
	snapshot, err := r.RaftLog.storage.Snapshot()
	// firstIndex
	if err == ErrSnapOutOfDate {
		log.Panicf("sendSnapshot require an out-dated snapshot")
	} else if err == ErrSnapshotTemporarilyUnavailable {
		return false
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	// sendSnapshot succeed , avoid duplicate sendSnapshot before handleAppResp
	r.Prs[to].SnapState, r.Prs[to].SnapshotTicker, r.Prs[to].SnapLastIndex = SnapSending, 0, snapshot.Metadata.Index

	r.msgs = append(r.msgs, msg)
	// prevent from : after follower apply snapshot , Leader send some entries which before snapshot.Meta.index
	r.Prs[to].Next = snapshot.Metadata.Index + 1

	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		// Todo: can't imagine a legal circumstance
		log.Info("[ERROR] sendAppend while r.state!=StateLeader")
		panic(errors.New("sendAppend while r.state!=StateLeader"))
	}
	if r.id == to {
		log.Info("[ERROR] sendAppend to Node itself")
		panic(errors.New("sendAppend to Node itself"))
	}
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	// index->log has been compact. send snapshot.
	if err != nil || prevLogIndex < r.RaftLog.FirstIndex()-1 {
		log.Info("[DEBUG] send snapshot because leader compact raftlog ")
		return r.sendSnapshot(to)
	}
	entriesToSend := make([]*pb.Entry, 0)
	lastIndex := r.RaftLog.LastIndex()
	if r.Prs[to].Next > lastIndex {
		entriesToSend = nil
	} else if r.Prs[to].Next < r.RaftLog.FirstIndex() {
		panic("sendAppend Next<FirstIndex(), should check when checkSendSnapshot")
	} else {
		for i := int(r.Prs[to].Next) - int(r.RaftLog.FirstIndex()); i < int(lastIndex)-int(r.RaftLog.FirstIndex())+1; i++ {
			entriesToSend = append(entriesToSend, &r.RaftLog.entries[i])
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.Lead,
		To:      to,
		Term:    r.Term,
		Entries: entriesToSend,
		Commit:  r.RaftLog.committed,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
	}
	r.msgs = append(r.msgs, msg)
	// Note: this part's primary construction is imeffective
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// log.Info("tick().")
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.Prs[r.id].recentAlive = true
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.handleCheckQuorum()
		}
		if r.State != StateLeader {
			return
		}
		if r.leadTransferee != None {
			// r.leadTransferee
			r.leadTransfereeElapsed++
			if r.leadTransfereeElapsed >= 2*r.electionTimeout {
				r.leadTransfereeElapsed = 0
				r.leadTransferee = None
			}
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.BcastHeartBeat(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
		for i := range r.Prs {
			if r.Prs[i].SnapState == SnapSending {
				r.Prs[i].SnapshotTicker++
				if r.Prs[i].SnapshotTicker >= r.electionTimeout {
					r.Prs[i].SnapState = SnapStateNormal
					r.Prs[i].SnapLastIndex = 0
				}
			}
		}
	default:
		r.electionElapsed++
		if r.electionRandomTimeout <= r.electionElapsed {
			r.electionElapsed = 0
			if _, ok := r.Prs[r.id]; ok {
				r.raiseElection()
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Vote = lead
	r.Lead = lead
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	// Todo: leaderAliveCheck can implement
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.Lead = None
	// self vote
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	r.State = StateCandidate
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// r.leadTransferee = None
	// r.leadTransfereeElapsed = 0
	// r.leaderAliveElapased = 0
	// r.haveSendedSnapShot = make(map[uint64]int)
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	}
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.leadTransfereeElapsed = 0
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	//---------------
	lastIndex := r.RaftLog.LastIndex()
	for i := range r.Prs {
		if uint64(i) == r.id {
			// debuginfo :+2 noopEntry
			r.Prs[i] = &Progress{Match: lastIndex + 1, Next: lastIndex + 2, recentAlive: true, SnapState: SnapStateNormal, SnapshotTicker: 0, SnapLastIndex: 0}
		} else {
			// note
			r.Prs[i] = &Progress{Match: 0, Next: lastIndex + 1, recentAlive: true, SnapState: SnapStateNormal, SnapshotTicker: 0, SnapLastIndex: 0}
		}
	}
	//---append noop entry to Leader's RaftLog
	noopEntry := &pb.Entry{EntryType: pb.EntryType_EntryNormal, Data: nil}
	r.AppendEntries([]*pb.Entry{noopEntry}...)
	r.BcastAppend()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// note left deal with Vote Request
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	canVote := (r.Vote == None && r.Lead == None) || (r.Vote == m.From)
	lastTerm, _ := r.RaftLog.LastTerm()
	isUptoDate := (m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex()))
	rejected := !(canVote && isUptoDate)
	if rejected {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	// m.From ==Lead RaiseElection
	r.electionElapsed = 0
	r.Vote = m.From
	r.msgs = append(r.msgs, msg)
}

// BcastAppend broadcast append message to all prs
func (r *Raft) BcastAppend() {
	if len(r.Prs) == 1 {
		lastIndex := r.RaftLog.LastIndex()
		r.RaftLog.committed = max(r.RaftLog.committed, lastIndex)
		return
	}
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
}

// AppendEntries append entries from ents... to local Raft
// only Leader uses this function
func (r *Raft) AppendEntries(entries ...*pb.Entry) error {
	if r.leadTransferee != None {
		return ErrProposalDropped
	}
	// Todo when will this happend
	if _, ok := r.Prs[r.id]; !ok {
		log.Warn("ERROR: AppendEntries r.id not appear in r.Prs, might change r.id")
		return ErrProposalDropped
	}
	lastIndex := r.RaftLog.LastIndex()
	for i, v := range entries {
		v.Index = lastIndex + uint64(i) + 1
		v.Term = r.Term
		// Todo(3A conf change)
		if v.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None && v.Index != r.PendingConfIndex {
				entries[i].EntryType = pb.EntryType_EntryNormal
				entries[i].Data = nil
			} else {
				r.PendingConfIndex = entries[i].Index
			}
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *v)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) <= 1 {
		r.RaftLog.committed = max(r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	return nil
}

// UpdateCommit update raft.committed Leader
func (r *Raft) UpdateCommit() bool {
	cntAll := 0
	var commits []uint64
	for _, v := range r.Prs {
		cntAll++
		commits = append(commits, v.Match)
	}
	sortkeys.Uint64s(commits)
	older := r.RaftLog.committed
	maybeNewCommit := commits[int((cntAll-1)/2)]
	if older >= maybeNewCommit {
		return false
	} else {
		maybeNewTerm, err := r.RaftLog.Term(maybeNewCommit)
		if err != nil {
			log.Info("UpdateCommit call Term with ErrCompacted or ErrUnavailable")
			return false
		} else {
			// Note: can't commit other Term's log entries
			if maybeNewTerm != r.Term {
				return false
			} else {
				r.RaftLog.committed = max(r.RaftLog.committed, maybeNewCommit)
				return true
			}
		}
	}
}

func (r *Raft) raiseElection() {
	r.becomeCandidate()
	// duplicate call becomeLeader when len(r.Prs)<=1
	// note logterm&Index in MessageSend
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.LastTerm()
	if err != nil {
		panic(err)
	}

	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      i,
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term, // note term&Logterm in (type pb.Message struct)
			Index:   lastLogIndex,
			LogTerm: lastLogTerm,
		})
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	var cntVote, cntAll uint64 = 0, 0
	for i := range r.votes {
		vote := r.votes[uint64(i)]
		cntAll++
		if vote {
			cntVote++
		}
	}
	// note >
	if cntVote*2 > uint64(len(r.Prs)) {
		// log.Infof("Election success. becomeLeader")
		r.becomeLeader()
	} else if (cntAll-cntVote)*2 > uint64(len(r.Prs)) {
		// log.Infof("Election failed. becomeFollower. %v %v %v", cntAll, cntVote, uint64(len(r.Prs)))
		r.becomeFollower(r.Term, None)
	}
	// else pending
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) StepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		// when Follower Receive a Propose Msg
		return ErrProposalDropped
	case pb.MessageType_MsgHup:
		r.raiseElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTimeoutNow:
		// log.Info("[DEBUG] MessageType_MsgTimeoutNow occur.")
		// log.Infof("%v receive MessageType_MsgTimeoutNow from %v", r.id, m.From)
		if _, ok := r.Prs[r.id]; ok {
			r.raiseElection()
		}
	case pb.MessageType_MsgTransferLeader:
		// log.Info("[DEBUG] MessageType_MsgTransferLeader occur.")
		if r.Lead == None {
			return nil
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	default:
	}
	return nil
}

func (r *Raft) StepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgHup:
		r.raiseElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		// log.Info("[DEBUG] MessageType_MsgTransferLeader occur.")
		if r.Lead == None {
			return nil
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgTimeoutNow:
		// log.Info("[ERROR] MessageType_MsgTimeoutNow occur.")
	default:
	}
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// if m.Term > r.Term {
	// follower think Leader's Term is bigger than Leader itself's. should not happend.
	// r.becomeFollower(m.Term, m.From)
	r.Prs[m.From].recentAlive = true
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	r.Prs[m.From].recentAlive = true
	if m.Reject {
		// Case1: if follower lack some logs
		nextIndex := m.Index
		// Case2: if follower lack some logs and append some other logs
		if m.LogTerm != None {
			for i := 0; i < len(r.RaftLog.entries); i++ {
				if r.RaftLog.entries[i].Term > m.LogTerm {
					if i > 0 && r.RaftLog.entries[i-1].Term == m.LogTerm {
						nextIndex = r.RaftLog.FirstIndex() + uint64(i)
						break
					}
				}
			}
		}
		r.Prs[m.From].Next = nextIndex
		r.sendAppend(m.From)
	} else {
		// Case3:
		if m.Index > r.Prs[m.From].Match {
			// filter
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
			if r.Prs[m.From].SnapState == SnapSending && r.Prs[m.From].Match >= r.Prs[m.From].SnapLastIndex {
				// apply successfully
				r.Prs[m.From].SnapState = SnapStateNormal
				r.Prs[m.From].SnapshotTicker = 0
				r.Prs[m.From].SnapLastIndex = 0
			}
			if r.UpdateCommit() {
				r.BcastAppend()
			}
			if m.Index == r.RaftLog.LastIndex() && r.leadTransferee == m.From {
				r.sendTimeout(r.leadTransferee)
			}
		}
	}
}

func (r *Raft) sendTimeout(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// m.from==Transferee
	if m.From == r.id || r.Prs[m.From] == nil {
		return
	}
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	log.Infof("%v start transferring to %v", r.id, m.From)
	r.leadTransferee = m.From
	// r.leadTransfereeTimeout = 0
	if r.RaftLog.LastIndex() == r.Prs[r.leadTransferee].Match {
		r.sendTimeout(r.leadTransferee)
	} else {
		r.sendAppend(r.leadTransferee)
	}
}

func (r *Raft) handleCheckQuorum() {
	countAlive := 0
	for i := range r.Prs {
		if r.Prs[i].recentAlive {
			countAlive++
			r.Prs[i].recentAlive = false
		}
	}
	r.Prs[r.id].recentAlive = true
	if countAlive <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) StepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgCheckQuorum:
		r.handleCheckQuorum()
	case pb.MessageType_MsgBeat:
		r.BcastHeartBeat(m)
	case pb.MessageType_MsgAppend:
		// if m.Term > r.Term , should becomeFollower and then handle Msg in StepFollower
		// if m.Term < r.Term , should send Reject
		// m.Term == r.Term should never happend
		// all in all, Leader should NOT append entries
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		} else if m.Term < r.Term {
			// might set From == None
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
		} else {
			log.Error("Error: Leader receive MsgAppend with the same Term, might have two Leader with the same Term.")
			panic(errors.New(" Leader receive MsgAppend with the same Term, might have two Leader with the same Term"))
		}
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		err := r.AppendEntries(m.Entries...)
		r.BcastAppend()
		return err
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	default:
	}
	return nil
}

func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A)
	switch {
	case m.Term == 0:
		// local message
		// note: include MsgTransferLeader
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// NOTE: every type of scaled Msg could be received. most of them should not be handled.
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgSnapshot:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
			return nil
		default:
			return nil
		}
	}
	switch r.State {
	case StateFollower:
		return r.StepFollower(m)
	case StateCandidate:
		return r.StepCandidate(m)
	case StateLeader:
		return r.StepLeader(m)
	default:
		return nil
	}
}

// handleAppendEntries handle AppendEntries RPC request. Type: MessageType_MsgAppend
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A)
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  false,
		Term:    r.Term,
		Index:   m.Index,
	}
	// there should not have MsgAppend which m.Term == None
	if m.Term == None {
		log.Warn("WARN: handleAppendEntries m.Term == None")
		panic(errors.New("handleAppendEntries m.Term == None"))
	}
	if StateCandidate == r.State {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Term < r.Term {
		// old leader send old Term's Entry to Append
		msg.Reject = true
		msg.Term = uint64(0)
		msg.Index = uint64(0)
		r.msgs = append(r.msgs, msg)
		return
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.electionElapsed = 0
	r.Lead = m.From

	// Case3: Follower doesn't have Leader's Log(m.Index==Leader's PrevLogIndex, >local's LastIndex)
	// should resend from LastIndex+1
	if m.Index > r.RaftLog.LastIndex() {
		msg.Reject = true
		msg.Index = r.RaftLog.LastIndex() + 1
		msg.LogTerm = None
		r.msgs = append(r.msgs, msg)
		return
	}

	// this overlap with Case3, should not happend
	localPrevLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		log.Info("ERROR: handleAppendEntries call Raftlog.term(prevLogIndex) can't find and return err")
		panic("handleAppendEntries call Raftlog.term(prevLogIndex) can't find and return err")
	}
	// Case2: Follower have other Term's Log (conflict)
	// should return localPrevLogTerm's first Log's Index&Term
	// search in r.RaftLog.entries
	if localPrevLogTerm != m.LogTerm {
		msg.Reject = true
		retLogIndex, retLogTerm := m.Index+1, localPrevLogTerm
		if len(r.RaftLog.entries) != 0 {
			for i := 0; i <= int(m.Index)-int(r.RaftLog.FirstIndex()); i++ {
				if r.RaftLog.entries[i].Term == localPrevLogTerm {
					retLogIndex = r.RaftLog.entries[i].Index
					break
				}
			}
		}
		msg.LogTerm, msg.Index = retLogTerm, retLogIndex
		r.msgs = append(r.msgs, msg)
		return
	}

	if len(r.RaftLog.entries) == 0 {
		for i := 0; i < len(m.Entries); i++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
		}
	} else {
		// NOTE: if local entries have more log than m.entries, [m.entries.last,local.entries.last] will not be truncated.
		for i, ent := range m.Entries {
			localTerm, err := r.RaftLog.Term(ent.Index)
			if err != nil || localTerm != ent.Term {
				// given TruncateIndex should >= Leader's committed Index >= Follower's committed Index >= applied Index
				if ent.Index < r.RaftLog.committed {
					log.Error("ERROR: handleAppendEntries m.Index < r.RaftLog.committed")
					panic("handleAppendEntries m.Index < r.RaftLog.committed")
				}
				if ent.Index < r.RaftLog.applied {
					log.Info("ERROR: Follower's Raftlog appliedIndex > Leader's PrevLogIndex")
					panic(errors.New(" Follower's Raftlog appliedIndex > Leader's PrevLogIndex"))
				}
				// 'if' should be [if m.index >= r.RaftLog.FirstIndex()-1], forgot -1
				if ent.Index < r.RaftLog.FirstIndex()-1 {
					log.Info("ERROR: handleAppendEntries try to append Entries which have been apply and compact")
					panic(fmt.Sprintf("handleAppendEntries try to append Entries which have been apply and compact %v %v", m.Index, r.RaftLog.FirstIndex()))
				}
				r.RaftLog.stabled = min(r.RaftLog.stabled, ent.Index-1)
				r.RaftLog.entries = r.RaftLog.entries[:int(ent.Index)-int(r.RaftLog.FirstIndex())]
				for j := i; j < len(m.Entries); j++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
				}
				break
			}
		}
	}
	msg.Index = r.RaftLog.LastIndex()
	// MsgAppend: update r.RaftLog.committed according m.Commit
	if r.RaftLog.committed < m.Commit {
		lastNewIndex := uint64(len(m.Entries)) + m.Index
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term < r.Term {
		msg.Reject = true
		// older Leader transfer to Follower
	} else if r.State != StateLeader {
		// Another Leader with the same Term
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) restoreSnapshot(snapshot *pb.Snapshot) {
	firstIndex := snapshot.Metadata.Index + 1

	r.RaftLog.entries = nil
	r.RaftLog.committed = firstIndex - 1
	// r.RaftLog.applied = firstIndex - 1
	// r.RaftLog.stabled = firstIndex - 1

	r.Prs = make(map[uint64]*Progress)
	nodes := snapshot.Metadata.ConfState.GetNodes()
	for i := range nodes {
		peer := nodes[i]
		r.Prs[peer] = &Progress{
			Next: 1,
		}
	}
	r.RaftLog.PendingSnapshot = snapshot
}

func (r *Raft) checkSnapshot(m pb.Message) bool {
	if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if m.Snapshot.Metadata.Index <= r.RaftLog.LastIndex() {
		if term, err := r.RaftLog.Term(m.Snapshot.Metadata.Index); err == nil && term == m.Snapshot.Metadata.Term {
			// snapshot already in follower's RaftLog
			r.RaftLog.committed = max(r.RaftLog.committed, m.Snapshot.Metadata.Index)
			return false
		}
	}
	return true
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snapshot := m.Snapshot
	if snapshot == nil {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if !r.checkSnapshot(m) {
		msg.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, msg)
		return
	}

	r.restoreSnapshot(snapshot)
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if r.Prs[id] != nil {
		return
	}
	r.Prs[id] = &Progress{
		Match:          0,
		Next:           r.RaftLog.LastIndex() + 1,
		recentAlive:    true,
		SnapState:      SnapStateNormal,
		SnapshotTicker: 0,
		SnapLastIndex:  0,
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	removeable := false
	if _, ok := r.Prs[id]; ok {
		removeable = true
	}
	// r.id== id can remove itself
	// or delete the whole cluster
	// removeable = removeable && (len(r.Prs) >= 1)
	if removeable {
		delete(r.Prs, id)
		r.PendingConfIndex = None
		if r.State == StateLeader && r.id != id {
			if r.UpdateCommit() {
				r.BcastAppend()
			}
		}
		if r.leadTransferee == id {
			r.leadTransferee = None
			r.leadTransfereeElapsed = 0
		}
	}
}

// add for 2AC
func (r *Raft) SoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) BcastHeartBeat(m pb.Message) {
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendHeartbeat(i)
	}
}
