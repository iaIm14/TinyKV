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
	// leaderAliveElapased   int
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
		id:      c.ID,
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower, //
		votes:   make(map[uint64]bool),

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	hardState, confState, _ := r.RaftLog.storage.InitialState()
	r.Term, r.Vote, r.RaftLog.committed = hardState.Term, hardState.Vote, hardState.Commit
	// Todo: might change regionID when recovery
	// if len(c.peers) != 0 {
	// 	for _, i := range c.peers {
	// 		if uint64(i) == r.id {
	// 			r.Prs[uint64(i)] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	// 		} else {
	// 			r.Prs[uint64(i)] = &Progress{Match: 0, Next: lastIndex + 1}
	// 		}
	// 	}
	// } else {
	// 	for _, i := range confState.Nodes {
	// 		if i == r.id {
	// 			r.Prs[i] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	// 		} else {
	// 			r.Prs[i] = &Progress{Match: 0, Next: lastIndex + 1}
	// 		}
	// 	}
	// }
	peers := c.peers
	if len(confState.Nodes) != 0 {
		peers = confState.Nodes
	}
	for _, i := range peers {
		if i == r.id {
			r.Prs[i] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[i] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

func (r *Raft) sendSnapshot(to uint64) bool {
	// _, pending := r.haveSendedSnapShot[to]
	// if pending {
	// return false
	// }
	snapshot, err := r.RaftLog.storage.Snapshot()
	// firstIndex
	if err != nil {
		return false
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	// debuginfo bug1
	// sendSnapshot succeed , avoid duplicate sendSnapshot before handleAppResp
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	// r.haveSendedSnapShot[to] = 0
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
	if r.Prs[to].Next == 0 {
		log.Info("[ERROR] sendAppend Prs[To].Next=0")
		// for i := range r.Prs {
		// 	log.Infof("id(%v).Next==(%v)", i, r.Prs[i].Next)
		// }
	}
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		// index->log has been compact. send snapshot.
		if err == ErrCompacted {
			log.Info("[DEBUG] send snapshot because leader compact raftlog ")
			return r.sendSnapshot(to)
		}
		// log.Infof("[ERROR] can't find prevLogIndex(%v) in r.raftlog , Should update to send Snapshot", prevLogIndex)
		return false
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.Lead,
		To:      to,
		Term:    r.Term,
		Entries: make([]*pb.Entry, 0),
		Commit:  r.RaftLog.committed,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
	}
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex >= r.Prs[to].Next {
		for i := r.Prs[to].Next; i <= lastIndex; i++ {
			if i < r.RaftLog.FirstIndex {
				// debuginfo
				continue
			}
			entry := &r.RaftLog.entries[i-r.RaftLog.FirstIndex]
			msg.Entries = append(msg.Entries, entry)
		}
		// appEntries = r.RaftLog.getEntries(r.Prs[to].Next, r.RaftLog.LastIndex()+1) //note +1: cannot pass TestLeaderycle2AA
		// for i := range appEntries {
		// 	msg.Entries = append(msg.Entries, &appEntries[i])
		// }
	} else if lastIndex+1 != r.Prs[to].Next {
		log.Infof("[ERROR] lastIndex(%v) < peers[%v].Next(%v)", lastIndex, to, r.Prs[to].Next)
	}
	// log.Infof("	[DEBUG] Append Entries Msg:%v", msg)
	if len(msg.Entries) != 0 {
		// log.Infof("DEBUG: %v==%v sendAppend to %v entries:%v", r.id, r.Lead, msg.To, msg.Entries)
		r.msgs = append(r.msgs, msg)
	} else {
		// log.Infof("DEBUG: %v==%v send emptyAppend to %v ,to update commit: %v", r.id, r.Lead, msg.To, msg.Commit)
		r.msgs = append(r.msgs, msg)
	}
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

// tickForElection Follower and Candidate tick handler
func (r *Raft) tickForElection() {
	// log.Info("tickForElection(). tick info: r.electionElapsed: %v ; r.electionRandomTimeout %v;r.electionTimeout %v;", r.electionElapsed, r.electionRandomTimeout, r.electionTimeout)
	if r.Term == 0 && len(r.Prs) == 0 {
		return
	}
	r.electionElapsed++
	if r.electionRandomTimeout <= r.electionElapsed {
		// log.Infof("[DEBUG]++++ tickForElection runs. send MsgHup")
		r.electionElapsed = 0
		r.Step(pb.Message{
			// debuginfo
			// From:    r.id,
			// To:      r.id,
			MsgType: pb.MessageType_MsgHup,
		})
	}
}

// func (r *Raft) tickLeaderAlive() {
// 	r.leaderAliveElapased++
// 	for i := range r.Communicate {
// 		r.Communicate[i]++
// 		if r.Communicate[i]*9 >= r.electionTimeout*10 {
// 			delete(r.Communicate, i)
// 		}
// 	}
// 	if r.leaderAliveElapased >= r.electionTimeout*2 {
// 		r.leaderAliveElapased = 0
// 		if len(r.Communicate)*2 < len(r.Prs) {
// 			// AliveCheck NotPass
// 			r.becomeFollower(r.Term, None)
// 		}
// 	}
// }

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// log.Info("tick().")
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		if r.leadTransferee != None {
			// r.leadTransferee
			r.leadTransfereeElapsed++
			if r.leadTransfereeElapsed >= r.electionTimeout*2 {
				r.leadTransfereeElapsed = 0
				r.leadTransferee = None
			}
		}
		// r.tickLeaderAlive()
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// check quorum debug
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	default:
		r.tickForElection()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = None
	r.leadTransfereeElapsed = 0
	// r.leaderAliveElapased = 0
	// Todo: leaderAliveCheck can implement
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// below should be deleted
	// if len(r.Prs) == 0 {
	// 	r.Prs[r.id] = &Progress{
	// 		Match: r.RaftLog.LastIndex(),
	// 		Next:  r.RaftLog.LastIndex() + 1,
	// 	}
	// 	r.Prs[lead] = &Progress{
	// 		Match: 0,
	// 		Next:  1,
	// 	}
	// }
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
	r.leadTransferee = None
	r.leadTransfereeElapsed = 0
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
	// debug Leader->leader no operation?
	if r.State == StateLeader {
		log.Info("[WARN] becomeLeader node already been leader")
		panic("becomeLeader node already been leader")
	}

	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.leadTransfereeElapsed = 0
	// r.leaderAliveElapased = 0
	// r.haveSendedSnapShot = make(map[uint64]int)
	// r.Communicate = make(map[uint64]int)
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	//---------------
	lastIndex := r.RaftLog.LastIndex()
	for i := range r.Prs {
		if uint64(i) == r.id {
			// debuginfo :+2 noopEntry
			r.Prs[i] = &Progress{Match: lastIndex + 1, Next: lastIndex + 2}
		} else {
			// note
			r.Prs[i] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	//---append noop entry to Leader's RaftLog
	noopEntry := &pb.Entry{EntryType: pb.EntryType_EntryNormal, Data: nil}
	r.AppendEntries([]*pb.Entry{noopEntry}...)
	r.BcastAppend()
	// log.Infof("[DEBUG] in BecomeLeader lastIndex:%v", lastIndex)
	// for _, v := range r.RaftLog.entries {
	// 	log.Infof("	[DEBUG] Leader Entries:%v", v)
	// }
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
	if m.Term != None && m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	canVote := (r.Vote == None && r.Lead == None) || (r.Vote == m.From)
	lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	isUptoDate := (m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())) && err == nil
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
func (r *Raft) AppendEntries(entries ...*pb.Entry) {
	if r.leadTransferee != None {
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	for i, v := range entries {
		v.Index = lastIndex + uint64(i) + 1
		v.Term = r.Term
		if v.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				// log.Infof("[DEBUG] PendingConfIndex:%v r.raftlog.applied:%v, ent.index:%v", r.PendingConfIndex, r.RaftLog.applied, v.Index)
				if r.PendingConfIndex > r.RaftLog.applied {
					v.EntryType = pb.EntryType_EntryNormal
					v.Data = make([]byte, 0)
				}
			} else {
				r.PendingConfIndex = v.Index
				// debuginfo: old pendingConfEntry:MsgType still PendingConfIndex
			}
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *v)
	}
	// log.Infof("[DEBUG]+ noopEntry: %v; r.id:%v", entries, r.id)
	// log.Info("[DEBUG]+ prs:")
	// for i, v := range r.Prs {
	// 	log.Info("id:", i, " Match:", v.Match, " Next:", v.Next)
	// }
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) <= 1 {
		r.RaftLog.committed = max(r.RaftLog.committed, r.RaftLog.LastIndex())
	}
}

// UpdateCommit update raft.committed Leader
func (r *Raft) UpdateCommit() bool {
	cntAll := 0
	var commits []uint64
	for _, v := range r.Prs {
		cntAll++
		commits = append(commits, v.Match)
	}
	// log.Infof("	+++[DEBUG] commits[]:%v", commits)
	// for i := range r.Prs {
	// 	log.Infof("Node(%v) Match(%v)", i, r.Prs[i].Match)
	// }
	sortkeys.Uint64s(commits)
	older := r.RaftLog.committed
	maybeNewCommit := commits[int((cntAll-1)/2)]
	if older >= maybeNewCommit {
		// debuginfo >=
		// log.Info("elder commit & newly commit id is the same. committed: %v", r.RaftLog.committed)
		return false
	} else {
		if maybeNewCommit > r.RaftLog.LastIndex() {
			log.Infof("ERROR: %v %v", r.RaftLog.LastIndex(), maybeNewCommit)
		}
		maybeNewTerm, err := r.RaftLog.Term(maybeNewCommit)
		if err != nil {
			return false
		} else {
			if maybeNewTerm != r.Term {
				// log.Info("[DEBUG] different Term when update commit: Try to Commit Entries from earlier Term. r.Term:%v NewTerm:%v", r.Term, maybeNewTerm)
				return false
			} else {
				r.RaftLog.committed = max(r.RaftLog.committed, maybeNewCommit)
				// log.Infof("	---[DEBUG] update r.raftlog.committed into : %v", r.RaftLog.committed)
				return true
			}
		}

	}
}

func (r *Raft) raiseElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	}
	// note logterm&Index in MessageSend
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.LastTerm()

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
	// log.Infof("[DEBUG] Candidate receive MsgRequestVoteResponse from %v", m.From)
	if m.Term != None && m.Term < r.Term {
		return
	}
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
		r.becomeFollower(r.Term, r.Lead)
	} else {
		// log.Infof("Election still pending. cntAll & cntVote :%v %v", cntAll, cntVote)
		// log.Infof("Election situation: !!! %v", r.votes)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) StepFollower(m pb.Message) error {
	switch m.MsgType {
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
	case pb.MessageType_MsgRequestVoteResponse:
		// log.Infof("[ERROR] Follower receive MsgRequestVoteResponse from %v", m.From)
	case pb.MessageType_MsgTimeoutNow:
		// log.Info("[DEBUG] MessageType_MsgTimeoutNow occur.")
		// log.Infof("%v receive MessageType_MsgTimeoutNow from %v", r.id, m.From)
		r.raiseElection()
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
	case pb.MessageType_MsgHup:
		r.raiseElection()
	case pb.MessageType_MsgAppend:
		// debuginfo
		// if m.Term == m.From {
		// 	r.becomeFollower(m.Term, m.From)
		// }
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
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
	if m.Term > r.Term {
		// debug m.From
		r.becomeFollower(m.Term, m.From)
		// log.Infof("[ERROR] HeartBeatResp from higher term's follower(%v).", m.From)
	} else {
		// debugnote
		// r.Communicate[m.From] = 0
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			// log.Infof("Follower %v 's log is out-dated. match %v , leader(%v)'s lastindex=%v", m.From, r.Prs[m.From].Match, r.id, r.RaftLog.LastIndex())
			// r.Prs[m.From].Match = min(r.Prs[m.From].Match, m.Commit)
			// r.Prs[m.From].Next = r.Prs[m.From].Match + 1
			r.sendAppend(m.From)
		} else {
			// log.Infof("Leader receive empty HeartBeatResp from %v", m.From)
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	// r.Communicate[m.From] = 0
	// delete(r.haveSendedSnapShot, m.From)
	if m.Reject {
		// log.Infof("[DEBUG] m.Index & r.prs[from].Next: %v %v", m.Index, r.Prs[m.From].Next)
		// if m.Index == r.Prs[m.From].Next-1 {
		// if: ignore communication delay messages
		nextIndex := m.Index
		// m.Logterm==None return nextIndex(Peer.lastIndex+1)
		if m.LogTerm != None {
			for i := 0; i < len(r.RaftLog.entries); i++ {
				if r.RaftLog.entries[i].Term > m.LogTerm {
					if i > 0 && r.RaftLog.entries[i-1].Term == m.LogTerm {
						nextIndex = r.RaftLog.FirstIndex + uint64(i)
						break
					}
				}
			}
		}
		r.Prs[m.From].Next = nextIndex
		// log.Infof("[DEBUG] receive MsgAppendResp reject. send again from entries.Index:%v logterm: %v", r.Prs[m.From].Next, m.LogTerm)
		r.sendAppend(m.From)
	} else {
		// log.Infof("leader(%v) index:%v match[%v]:%v", r.id, m.Index, m.From, r.Prs[m.From].Match)
		if m.Index > r.Prs[m.From].Match {
			// filter
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
			if r.UpdateCommit() {
				r.BcastAppend()
			}
			if m.Index == r.RaftLog.LastIndex() && r.leadTransferee != None && r.leadTransferee == m.From {
				r.sendTimeout(r.leadTransferee)
				// debuginfo
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
	if r.leadTransferee != None {
		if r.leadTransferee == m.From {
			return
		} else {
			// Node before can't be leader(no resp)
			// debuginfo
			// return
		}
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

func (r *Raft) StepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.BcastHeartBeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		r.AppendEntries(m.Entries...)
		// if len(r.RaftLog.entries) > 10 {
		// log.Infof("%v==%v propose msg :%v lastIndex:%v last ten raftlog entries:%v", r.id, r.Lead, m.Entries, r.RaftLog.LastIndex(), r.RaftLog.entries[len(r.RaftLog.entries)-10:])
		// } else {
		// log.Infof("%v==%v propose msg :%v lastIndex:%v all raftlog entries: %v", r.id, r.Lead, m.Entries, r.RaftLog.LastIndex(), r.RaftLog.entries[0:])
		// }
		r.BcastAppend()
	case pb.MessageType_MsgHup:
		// log.Info("[ERROR] Leader receive local MsgHup")
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgSnapshot:
		// log.Info("[ERROR] leader receive MessageType_MsgSnapshot.")
		r.handleSnapshot(m)
	default:
	}
	return nil
}

func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A)
	if r.id != None && r.Prs[r.id] == nil && len(r.Prs) != 0 {
		return nil
	}
	// log.Infof("[DEBUG] INTO Step1 Phase. message: %v", m)
	switch {
	case m.Term == 0:
		// local message
		// note: include MsgTransferLeader
		// no operation?
	case m.Term > r.Term:
		// log.Infof("%v become Follower because Term diff: m & r.Term: %v %v", r.id, m.Term, r.Term)
		// debugnote: no valid leader
		r.becomeFollower(m.Term, None)
	}
	// log.Info("[DEBUG] INTO 2 Phase.")
	switch r.State {
	case StateFollower:
		r.StepFollower(m)
	case StateCandidate:
		r.StepCandidate(m)
	case StateLeader:
		r.StepLeader(m)
	}
	return nil
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
	if m.Term != None && m.Term < r.Term {
		// old leader send old Term's Entry to Append
		// debug: maybe send back to leader ,(leader->follower->leader)
		// debuginfo
		msg.Reject = true
		msg.Term = uint64(0)
		msg.Index = uint64(0)
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Index > r.RaftLog.LastIndex() {
		msg.Reject = true
		msg.Index = r.RaftLog.LastIndex() + 1
		msg.LogTerm = None
		r.msgs = append(r.msgs, msg)
		// if 0 > len(r.RaftLog.entries)-10 {
		// 	log.Infof("DEBUG1: %v AppendEntries: %v", r.id, r.RaftLog.entries[0:])
		// } else {
		// 	log.Infof("DEBUG1: %v last ten AppendEntries: %v", r.id, r.RaftLog.entries[uint(len(r.RaftLog.entries)-10):])
		// }
		return
	}
	// situation: m.Term>r.Term
	// handleHeartbeat :make sure Term same
	r.electionElapsed = 0
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	r.Term, msg.Term = m.Term, m.Term
	if r.State == StateCandidate || r.State == StateLeader {
		r.becomeFollower(m.Term, m.From)
	}
	// ---------------check Term&Index------
	if m.Index >= r.RaftLog.FirstIndex {
		// log.Infof("[DEBUG]+++ %v %v", m.Index, r.RaftLog.FirstIndex)
		t, err := r.RaftLog.Term(m.Index)
		if err != nil {
			log.Info("[ERROR] return err!=nil .Term() should not hapend")
			msg.Reject = true
			r.msgs = append(r.msgs, msg)
			return
		}
		if t != m.LogTerm {
			msg.Reject = true
			retLogIndex, retLogTerm := m.Index+1, t
			for i := 0; i <= int(m.Index-r.RaftLog.FirstIndex); i++ {
				if r.RaftLog.entries[i].Term == t {
					retLogIndex = r.RaftLog.entries[i].Index
					break
				}
			}
			msg.LogTerm, msg.Index = retLogTerm, retLogIndex
			r.msgs = append(r.msgs, msg)
			// if 0 > len(r.RaftLog.entries)-10 {
			// 	log.Infof("DEBUG2: %v AppendEntries: %v", r.id, r.RaftLog.entries[0:])
			// } else {
			// 	log.Infof("DEBUG2: %v last ten AppendEntries: %v", r.id, r.RaftLog.entries[uint(len(r.RaftLog.entries)-10):])
			// }
			return
		}
	}
	var entries []*pb.Entry
	conflict := false
	for i, entry := range m.Entries {
		if entry.Index < r.RaftLog.FirstIndex {
			continue
		}
		if entry.Index > r.RaftLog.LastIndex() {
			entries = append(entries, m.Entries[i:]...)
			break
		}
		// index&Term not match in situation [entry.index>=firstIndex]
		// m.entries[0].Index==m.index+1 (consistency check)
		t, err := r.RaftLog.Term(entry.Index)
		if err != nil {
			// raftlog.term() ensures this not hapend.
			log.Info("[ERROR] Msg.Entries[i].Index search Term error.")
			return
		}
		if t != entry.Term {
			conflict = true
		}
		if conflict {
			entries = append(entries, entry)
		}
	}
	// r.RaftLog.BaseAppendEntries(entries...)
	// log.Infof("[DEBUG+] %v", entries)
	if len(entries) != 0 {
		for i := range entries {
			entry := entries[i]
			idx, term := entry.Index, entry.Term
			if idx > r.RaftLog.LastIndex() {
				for j := range entries[i:] {
					r.RaftLog.entries = append(r.RaftLog.entries, *entries[j])
				}
				r.RaftLog.stabled = min(r.RaftLog.stabled, entries[0].Index-1)
				break
			}
			t, err := r.RaftLog.Term(idx)
			if err != nil {
				panic(err)
			}
			if t != term {
				r.RaftLog.entries = r.RaftLog.entries[:idx-r.RaftLog.FirstIndex]
				for j := range entries[i:] {
					r.RaftLog.entries = append(r.RaftLog.entries, *entries[j])
				}
				r.RaftLog.stabled = min(r.RaftLog.stabled, entries[0].Index-1)
				break
			}
		}
	}
	msg.Index = r.RaftLog.LastIndex()
	// MsgAppend: update r.RaftLog.committed
	if r.RaftLog.committed < m.Commit {
		lastNewIndex := uint64(len(m.Entries)) + m.Index
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
	r.msgs = append(r.msgs, msg)
	// if 0 > len(r.RaftLog.entries)-10 {
	// 	log.Infof("DEBUG3: %v AppendEntries: %v m.index=%v", r.id, r.RaftLog.entries[0:], m.Index)
	// } else {
	// 	log.Infof("DEBUG3: %v last ten AppendEntries: %v, m.index=%v", r.id, r.RaftLog.entries[uint(len(r.RaftLog.entries)-10):], m.Index)
	// }
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// debuginfo
	// if m.Term == r.Term && r.State == StateCandidate {
	// 	r.becomeFollower(m.Term, m.From)
	// }
	// if r.State == StateLeader {
	// 	log.Infof("[ERROR] leader(%v) receive MsgHeartBeat from (%v)", r.id, m.From)
	// }
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term != None && m.Term < r.Term {
		msg.Reject = true
		// older Leader transfer to Follower
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, msg)
	// log.Infof("[DEBUG]handle MsgHeartBeat: m.Commit & r.RaftLog.HeartBeat:%v %v", m.Commit, r.RaftLog.committed)
	// debuginfo
	// if m.Commit > r.RaftLog.committed {
	// 	r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	// }
}

func (r *Raft) restoreSnapshot(snapshot *pb.Snapshot) {
	firstIndex := snapshot.Metadata.Index + 1
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.FirstIndex = firstIndex
	r.RaftLog.applied = firstIndex - 1
	r.RaftLog.committed = firstIndex - 1
	r.RaftLog.stabled = firstIndex - 1

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
	// debuginfo
	r.becomeFollower(max(m.Term, r.Term), m.From)
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
		Match: 0,
		Next:  1,
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	removeable := (r.Prs[id] != nil)
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
