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
	"math/rand"
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
	electionTimeout       int
	electionRandomTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

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
		Term:    0, // debug initial term
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower, //
		votes:   make(map[uint64]bool),

		heartbeatTimeout: c.HeartbeatTick, // + rand.Intn(10),
		electionTimeout:  c.ElectionTick,  // + rand.Intn(10), // debug Tick & Timeout rand
	}
	for _, v := range c.peers {
		r.Prs[v] = &Progress{
			Match: 0,
			Next:  1,
			// debug initial
		}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	var entries []*pb.Entry
	prevLogIndex := r.Prs[to].Next - 1
	var prevLogTerm uint64 // note
	for _, v := range r.RaftLog.entries {
		if v.Index == prevLogIndex {
			prevLogTerm = v.Term
			break
		}
	}
	// note initialState RaftLog.Entries[] empty
	for _, v := range r.RaftLog.entries {
		index := v.Index
		if index > prevLogIndex {
			entries = append(entries, &v)
			r.Prs[to].Next = index + 1
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.Lead,
		To:      to,
		Term:    r.Term,
		Entries: entries,
		Commit:  r.RaftLog.committed,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	//commit := min(r.Prs[to].Match, r.RaftLog.committed)
	//debug why send commit
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id, // debug or r.Lead
		//Commit:  commit, // note not send
		To:   to,
		Term: r.Term,
	}
	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionRandomTimeout <= r.electionElapsed {
			r.electionElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				To:      r.id, // note transfer to local
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionRandomTimeout <= r.electionElapsed {
			r.electionElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				To:      r.id, // note transfer to local
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// check quorum debug
			r.Step(pb.Message{
				From:    r.id,
				To:      r.Lead,
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.Vote = 0 // update VoteCounter debug
	// update Vote From debug
	for i := range r.votes {
		delete(r.votes, i)
	}
	r.State = StateFollower
	r.electionElapsed = 0
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		return
	}
	r.State = StateCandidate
	r.Term++ // reset
	// self vote
	r.Vote = 1
	for i, _ := range r.votes {
		delete(r.votes, i)
	}
	r.votes[r.id] = true
	r.electionElapsed = 0 // note
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// debug write log
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// debug Leader->leader no operation?
	if r.State == StateFollower || r.State == StateLeader {
		return
	}
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0 // easy left
	r.electionRandomTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
	for i, _ := range r.Prs {
		r.sendAppend(i)
	}
	// r.vote counter debug
	//r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
	//	EntryType: pb.EntryType_EntryNormal,
	//	Term:      r.Term,
	//	Index:     r.RaftLog.LastIndex() + 1,
	//})
	//for i, _ := range r.Prs {
	//	r.Prs[i].Next = r.RaftLog.LastIndex() + 1
	//	r.Prs[i].Match = 0
	//} // debug deal with Next&Match

}

func (r *Raft) DealWithRequestVote(m pb.Message) {
	// note left deal with Vote Request
	length := len(r.RaftLog.entries)
	if r.Term > m.Term || (length != 0 && r.RaftLog.entries[length-1].Term > m.LogTerm) {
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term, // reject & update term?
			Reject:  true,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
		})
		return
	} else if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// m.From ==Lead RaiseElection
	r.electionElapsed = 0
	if r.Vote == 0 || r.Vote == m.From {
		r.Vote = m.From
	} else {
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term, // reject & update term?
			Reject:  true,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
		})
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  false,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if len(r.Prs) == 1 { // boundary
				r.becomeCandidate()
				r.becomeLeader()
			} else {
				r.becomeCandidate()
				// note logterm&Index in MessageSend
				lastIndex := r.RaftLog.LastIndex()
				var logTerm uint64
				if len(r.RaftLog.entries) == 0 {
					logTerm = 0
				} else {
					logTerm = r.RaftLog.entries[len(r.RaftLog.entries)-1].Term
				}
				for i, _ := range r.Prs {
					if i == r.id {
						continue
					}
					r.msgs = append(r.msgs, pb.Message{
						From:    r.id,
						To:      i,
						MsgType: pb.MessageType_MsgRequestVote,
						Term:    r.Term, // note term&Logterm in (type pb.Message struct)
						Index:   lastIndex,
						LogTerm: logTerm,
					})
				}
			}
		case pb.MessageType_MsgAppend:
			r.Lead = m.From
			r.electionElapsed = 0
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.DealWithRequestVote(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		default:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if len(r.Prs) == 1 { // boundary
				r.becomeLeader()
			} else {
				r.becomeCandidate()
				// note logterm&Index in MessageSend
				lastIndex := r.RaftLog.LastIndex()
				var logTerm uint64
				if len(r.RaftLog.entries) == 0 {
					logTerm = 0
				} else {
					logTerm = r.RaftLog.entries[len(r.RaftLog.entries)-1].Term
				}
				for i, _ := range r.Prs {
					if i == r.id {
						continue
					}
					r.msgs = append(r.msgs, pb.Message{
						From:    r.id,
						To:      i,
						MsgType: pb.MessageType_MsgRequestVote,
						Term:    r.Term, // note term&Logterm in (type pb.Message struct)
						Index:   lastIndex,
						LogTerm: logTerm,
					})
				}
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.DealWithRequestVote(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgRequestVoteResponse:
			if r.Term < m.Term {
				// debug r.Lead not right
				r.becomeFollower(m.Term, r.Lead)
				break
			}
			r.Vote++
			if m.Reject == false {
				r.votes[m.From] = true
			}
			var cntVote uint64 = 0
			for i, _ := range r.votes {
				if r.votes[i] == true {
					cntVote++
				}
			}
			// >= or > debug left
			if cntVote*2 >= uint64(len(r.Prs)) {
				r.becomeLeader()
			} else if (r.Vote-cntVote)*2 > uint64(len(r.Prs)) {
				r.becomeFollower(r.Term, r.Lead)
			}
		case pb.MessageType_MsgTimeoutNow:
		default:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for i, _ := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendHeartbeat(i)
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeatResponse:
			if m.Term > r.Term {
				// debug m.From
				r.becomeFollower(m.Term, m.From)
				// note left
			}
		case pb.MessageType_MsgPropose:
			for i, _ := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				// write into local Raftlog
			}
			for i, _ := range r.Prs {
				if i == r.id {
					continue
				}
				r.msgs = append(r.msgs, pb.Message{
					From:    r.id,
					To:      i,
					Entries: m.Entries,
					MsgType: pb.MessageType_MsgAppend,
				})
				r.Prs[i].Next += uint64(len(m.Entries))
				// debug note deal with Next&Match change
			}
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgRequestVote:
			r.DealWithRequestVote(m)
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgAppendResponse:
			if m.Term > r.Term {
				// note left m.From not right
				r.becomeFollower(m.Term, m.From)
			}
			if m.Reject == true {
				r.Prs[m.From].Next = m.Index
				//left
			} else {
				// left debug
				r.Prs[m.From].Next = m.Index
				r.Prs[m.From].Match = m.Index
				// r.RaftLog.committed
			}
		case pb.MessageType_MsgSnapshot:
		default:
			// pb.MessageType_MsgRequestVoteResponse && pb.MessageType_MsgTimeoutNow
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//r.electionElapsed=0 note
	r.Lead = m.From
	if r.Term < m.Term || (r.Term == m.Term && r.State != StateFollower) {
		r.becomeFollower(m.Term, m.From)
	}
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  false,
		Term:    r.Term,
		Index:   m.Index,
	}
	if r.Term > m.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	// ---------------check Term&Index------
	if t, err := r.RaftLog.Term(m.Index); err != nil || t != m.LogTerm {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Commit < r.RaftLog.committed {
		msg.Commit = min(m.Index+uint64(len(m.Entries)), m.Commit)
	}
	var entries []*pb.Entry
	conflict := false
	for _, entry := range m.Entries {
		t, err := r.RaftLog.Term(entry.Index)
		if err != nil {
			panic(err) //
		}
		if t != entry.Term {
			conflict = true
		}
		if conflict {
			entries = append(entries, entry)
		}
	}
	//--append to RaftLog

	for i := 0; i < len(m.Entries); i++ {
		entry := m.Entries[int(m.Index)+1+i]
		if entry.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else {
			if r.RaftLog.entries[entry.Index].GetTerm() != entry.GetTerm() {
				r.RaftLog.entries[entry.Index] = *entry
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			} else {
			}
		}
	}
	if m.Commit > r.RaftLog.committed {
		// note c=min(a,b) debug
		lastNewIndex := m.Index + uint64(len(m.Entries))
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Reject:  true,
			MsgType: pb.MessageType_MsgHeartbeatResponse,
		})
	} else {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, r.Lead)
		}
		r.electionElapsed = 0
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
