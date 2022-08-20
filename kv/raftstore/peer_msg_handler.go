package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/raft"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	// d.peer.readRaftCmds = append(d.peer.readRaftCmds, rd.ReadStates...)
	// if !d.IsLeader() {
	// 	d.peer.readProposals = nil
	// 	d.peer.readRaftCmds = nil
	// }
	ch := make(chan struct{})
	if raft.IsEmptySnap(&rd.Snapshot) {
		// apply entry Asynchronously.
		// base on tinykv's code frame, it is a little bit difficult to use a standalone
		// goroutine to handle apply. Maybe I will do this later
		// Todo: shall we apply entry when this ready has snapshot to install?
		go d.handleApplyCommited(rd.CommittedEntries, ch)
	}

	snapApplyRes, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if snapApplyRes != nil {
		d.ctx.storeMeta.Lock()
		d.SetRegion(snapApplyRes.Region)
		if len(snapApplyRes.PrevRegion.Peers) > 0 {
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: snapApplyRes.PrevRegion})
		}
		d.ctx.storeMeta.regions[snapApplyRes.Region.Id] = snapApplyRes.Region
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: snapApplyRes.Region})
		d.ctx.storeMeta.Unlock()
	}
	d.Send(d.ctx.trans, rd.Messages)

	if !raft.IsEmptySnap(&rd.Snapshot) {
		go d.handleApplyCommited(rd.CommittedEntries, ch)
	}
	<-ch
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) handleApplyCommited(entries []eraftpb.Entry, ch chan<- struct{}) {
	if len(entries) == 0 {
		// d.handleReadOnlyCmd(d.peerStorage.applyState.AppliedIndex)
		ch <- struct{}{}
		return
	}
	kvWB := &engine_util.WriteBatch{}
	cbs := make([]*message.Callback, 0)
	for _, entry := range entries {
		if entry.Index < d.peerStorage.applyState.AppliedIndex {
			continue
		}
		err, cb := d.process(&entry, kvWB)
		if d.stopped {
			ch <- struct{}{}
			return
		}
		if err != nil {
			panic(err)
		}
		if cb != nil {
			cbs = append(cbs, cb)
		}
	}
	if entries[len(entries)-1].Index > d.peerStorage.applyState.AppliedIndex {
		d.peerStorage.applyState.AppliedIndex = entries[len(entries)-1].Index
	}
	err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		panic(err)
	}
	err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		panic(err)
	}
	for _, cb := range cbs {
		cb.Done(nil) // resp was set in process
	}
	// d.handleReadOnlyCmd(d.peerStorage.applyState.AppliedIndex)
	ch <- struct{}{}
}

// handleReadOnlyCmd handle read requests that have readindex less than appliedIndex
// func (d *peerMsgHandler) handleReadOnlyCmd(appliedIndex uint64) {
// 	region := d.Region()
// LOOP:
// 	// for len(d.peer.readRaftCmds) != 0 && d.peer.readRaftCmds[0].ReadIndex <= appliedIndex {
// 	// 	data := d.peer.readRaftCmds[0].ReadRequest
// 	// 	d.peer.readRaftCmds = d.peer.readRaftCmds[1:]
// 		cb := d.findReadCallBack(data)
// 		if cb == nil {
// 			continue
// 		}
// 		req := raft_cmdpb.RaftCmdRequest{}
// 		err := req.Unmarshal(data[8:]) // timestamp int64
// 		if err != nil {
// 			panic(err)
// 		}

// 		if err = util.CheckRegionEpoch(&req, region, true); err != nil {
// 			cb.Done(ErrResp(err))
// 			continue
// 		}
// 		for _, r := range req.Requests {
// 			if key := util.GetKeyInRequest(r); key != nil {
// 				if err = util.CheckKeyInRegion(key, region); err != nil {
// 					cb.Done(ErrResp(err))
// 					continue LOOP
// 				}
// 			}
// 		}

// 		resp := newCmdResp()
// 		for _, cmd := range req.Requests {
// 			switch cmd.CmdType {
// 			case raft_cmdpb.CmdType_Get:
// 				val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, cmd.Get.Cf, cmd.Get.Key)
// 				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}})
// 			case raft_cmdpb.CmdType_Snap:
// 				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}})
// 				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
// 			}
// 		}
// 		cb.Done(resp)
// 	}
// }

func (d *peerMsgHandler) process(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) (error, *message.Callback) {
	// the entry has been checked for region or other err
	if entry.Data == nil {
		// noop entry
		return nil, nil
	}
	req := &raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var confChangeEntry eraftpb.ConfChange
		if err := confChangeEntry.Unmarshal(entry.Data); err != nil {
			return err, nil
		}
		if err := req.Unmarshal(confChangeEntry.Context); err != nil {
			return err, nil
		}
	} else {
		if err := req.Unmarshal(entry.Data); err != nil {
			return err, nil
		}
	}
	// check region epoch here
	if err := util.CheckRegionEpoch(req, d.Region(), true); err != nil {
		cb := d.findCallBack(entry.Index, entry.Term)
		if cb != nil {
			cb.Resp = ErrResp(err)
		}
		return nil, cb
	}

	if req.AdminRequest != nil {
		// process admin request
		return nil, d.processAdminRequest(entry, req, kvWB)
	} else if req.Requests != nil {
		return nil, d.processRequest(entry, req, kvWB)
	}
	return nil, nil
}

func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *message.Callback {
	cb := d.findCallBack(entry.Index, entry.Term)
	resp := newCmdResp()
	switch req.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactIndex, compactTerm := req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm
		if compactIndex >= d.LastCompactedIdx {
			d.peerStorage.applyState.TruncatedState.Index = compactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactTerm
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			// firstindex is not used in ScheduleCompactLog
			d.ScheduleCompactLog(0, compactIndex)
		}
	case raft_cmdpb.AdminCmdType_ChangePeer:
		changeReerCmd := req.AdminRequest.ChangePeer
		region := d.Region() // pointer
		idx := len(region.Peers)
		for i, p := range region.Peers {
			if p.Id == changeReerCmd.Peer.Id && p.StoreId == changeReerCmd.Peer.StoreId {
				idx = i
				break
			}
		}
		switch changeReerCmd.ChangeType {
		case eraftpb.ConfChangeType_AddNode:
			if idx == len(region.Peers) { // if not found in region peers
				d.ctx.storeMeta.Lock()
				region.Peers = append(region.Peers, changeReerCmd.Peer)
				region.RegionEpoch.ConfVer++
				d.ctx.storeMeta.setRegion(region, d.peer)
				d.ctx.storeMeta.Unlock()
				meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
				d.insertPeerCache(changeReerCmd.Peer)
			}
		case eraftpb.ConfChangeType_RemoveNode:
			if idx != len(region.Peers) { // if found in region peers
				d.ctx.storeMeta.Lock()
				region.Peers = append(region.Peers[:idx], region.Peers[idx+1:]...)
				region.RegionEpoch.ConfVer++
				d.ctx.storeMeta.setRegion(region, d.peer)
				d.ctx.storeMeta.Unlock()
				meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
				if d.peer.Meta.Id == changeReerCmd.Peer.Id {
					d.destroyPeer()
					return cb
				}
				// since we process msg async, msg that make target peer to destroy itself might not sent, if we remove cache
				//d.removePeerCache(changeReerCmd.Peer.Id)
			}
		}
		var cc eraftpb.ConfChange
		_ = cc.Unmarshal(entry.Data)
		d.RaftGroup.ApplyConfChange(cc)
		resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()}}
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		split := req.AdminRequest.Split
		if err := util.CheckKeyInRegion(split.SplitKey, region); err != nil {
			if cb != nil {
				cb.Resp = ErrResp(err)
			}
			return cb
		}

		region.RegionEpoch.Version++
		newPeers := make([]*metapb.Peer, 0, len(split.NewPeerIds))
		// peers in newRegion has same store id to current peers
		for i, p := range region.Peers {
			newPeers = append(newPeers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: p.StoreId})
		}
		newRegion := &metapb.Region{
			Id:          split.NewRegionId,
			StartKey:    util.SafeCopy(split.SplitKey),
			EndKey:      util.SafeCopy(region.EndKey),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: region.RegionEpoch.ConfVer, Version: region.RegionEpoch.Version},
			Peers:       newPeers,
		}
		region.EndKey = util.SafeCopy(split.SplitKey)
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[region.Id] = region
		d.ctx.storeMeta.regions[newRegion.Id] = newRegion
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		d.ctx.storeMeta.Unlock()
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		for _, p := range newPeers {
			newPeer.insertPeerCache(p)
		}
		newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		d.ctx.router.register(newPeer)
		// see in startworkers & maybeCreateWorkers
		_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{region, newRegion},
			},
		}
	}
	if cb != nil {
		cb.Resp = resp
	}
	return cb
}
func GetKeyInRequest(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	default:
		return nil
	}
}

// processRequest process normal cmd that only write to db, since readonly cmd well be handled by readIndex
func (d *peerMsgHandler) processRequest(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *message.Callback {
	cb := d.findCallBack(entry.Index, entry.Term)
	resp := newCmdResp()
	region := d.Region()
	// check key in region, since there might be region split request when processing cmd
	for _, r := range req.Requests {
		if key := GetKeyInRequest(r); key != nil {
			if err := util.CheckKeyInRegion(key, region); err != nil {
				if cb != nil {
					BindRespError(resp, err)
					cb.Resp = resp
				}
				return cb
			}
		}
	}

	for _, cmd := range req.Requests {
		switch cmd.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(cmd.Put.Cf, cmd.Put.Key, cmd.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}})
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(cmd.Delete.Cf, cmd.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}})
		}
	}
	if cb != nil {
		cb.Resp = resp
	}
	return cb
}

func (d *peerMsgHandler) findCallBack(index, term uint64) *message.Callback {
	for {
		if len(d.peer.proposals) == 0 {
			return nil
		}
		prop := d.peer.proposals[0]
		if term < prop.term {
			return nil
		}
		d.peer.proposals = d.peer.proposals[1:]
		if prop.term == term && prop.index == index {
			return prop.cb
		}
		NotifyStaleReq(prop.term, prop.cb)
	}
}

func (d *peerMsgHandler) findReadCallBack(ctx []byte) *message.Callback {
	for {
		if len(d.peer.readProposals) == 0 {
			return nil
		}
		prop := d.peer.readProposals[0]
		d.peer.readProposals = d.peer.readProposals[1:]
		// if bytes.Equal(ctx, prop.readCmd) {
		// 	return prop.cb
		// }
		NotifyStaleReq(prop.term, prop.cb)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		// propose admin req
		d.proposeAdminRequest(msg, cb)
	}
	if len(msg.Requests) != 0 {
		// propose req
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		nidx := d.nextProposalIndex()
		if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.peer.proposals = append(d.peer.proposals, &proposal{index: nidx, term: d.Term(), cb: cb})
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.applyState.AppliedIndex {
			cb.Done(ErrResp(errors.New("there is another config change pending")))
			return
		}
		ctx, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		nidx := d.nextProposalIndex()
		err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    ctx,
		})
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.peer.proposals = append(d.peer.proposals, &proposal{index: nidx, term: d.Term(), cb: cb})
	case raft_cmdpb.AdminCmdType_Split:
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		nidx := d.nextProposalIndex()
		if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.peer.proposals = append(d.peer.proposals, &proposal{index: nidx, term: d.Term(), cb: cb})
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// prepropose guarantee that current peer is leader
	// should not mix read and write in one request
	// region := d.Region()
	// for _, req := range msg.Requests {
	// 	if key := util.GetKeyInRequest(req); key != nil {
	// 		if err := util.CheckKeyInRegion(key, region); err != nil {
	// 			cb.Done(ErrResp(err))
	// 			return
	// 		}
	// 	}
	// }

	read, write := false, false
	for _, req := range msg.Requests {
		if req.CmdType == raft_cmdpb.CmdType_Put || req.CmdType == raft_cmdpb.CmdType_Delete {
			write = true
		} else {
			read = true
		}
	}
	if read && write {
		panic("should not mix read and write in one request")
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	if read {
		t := time.Now().UnixNano()
		buf := bytes.NewBuffer([]byte{})
		err = binary.Write(buf, binary.LittleEndian, t)
		if err != nil {
			panic(err)
		}
		buf.Write(data)
		// d.RaftGroup.ReadIndex(buf.Bytes())
		// d.peer.readProposals = append(d.peer.readProposals, &readProposal{term: d.Term(), readCmd: buf.Bytes(), cb: cb})
	} else {
		nidx := d.nextProposalIndex()
		err = d.RaftGroup.Propose(data)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.peer.proposals = append(d.peer.proposals, &proposal{index: nidx, term: d.Term(), cb: cb})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	fmt.Printf("%+v\n", d.Region().Peers)
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
