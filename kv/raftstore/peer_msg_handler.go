package raftstore

import (
	"fmt"
	"math/rand"
	"time"

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
	"github.com/pingcap-incubator/tinykv/raft"
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
func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				handle(p)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) handleReadProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	for len(d.readProposals) > 0 {
		p := d.readProposals[0]
		if p.index == entry.Index {
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				handle(p)
			}
		}
		d.readProposals = d.readProposals[1:]
	}
}

func (d *peerMsgHandler) processRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	req := msg.Requests
	for i := range req {
		request := req[i]
		//apply to KV *badger.DB
		switch request.CmdType {
		case raft_cmdpb.CmdType_Delete:
			key := request.Delete.GetKey()
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				d.handleProposal(entry, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return
			}
			wb.DeleteCF(request.GetDelete().GetCf(), request.Delete.Key)
		case raft_cmdpb.CmdType_Put:
			key := request.Put.GetKey()
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				d.handleProposal(entry, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return
			}
			wb.SetCF(request.GetPut().GetCf(), request.Put.Key, request.Put.GetValue())
		default:
			panic("unexpected CmdType_Get or CmdType_Snap in processRequest")
		}
	}
	// find callback & send response to callback
	// wb append or new
	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		for i := range req {
			request := req[i]
			switch request.CmdType {
			case raft_cmdpb.CmdType_Get:
				panic("unexpected CmdType_Get in processRequest")
			case raft_cmdpb.CmdType_Snap:
				panic("unexpected CmdType_Snap in processRequest")
			case raft_cmdpb.CmdType_Put:
				// log.Infof("%v %v %v %v", d.regionId, d.peer, d.peerStorage.region.StartKey, d.peerStorage.region.EndKey)
				// d.SizeDiffHint += uint64(len(request.Put.Cf) + len(request.Put.Key) + len(request.Put.Value))
				// left
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}})
			case raft_cmdpb.CmdType_Delete:
				// log.Infof("regionid:%v peer:%v StartKey:%v EndKey:%v", d.regionId, d.peer, d.peerStorage.region.StartKey, d.peerStorage.region.EndKey)
				// d.SizeDiffHint -= uint64(len(request.Put.Cf) + len(request.Put.Key) + len(request.Put.Value))
				// left
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}})
			}
		}
		p.cb.Done(resp)
	})
}
func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := req.GetCompactLog()
		applySt := d.peerStorage.applyState
		if compactLog.CompactIndex >= applySt.TruncatedState.Index {
			applySt.TruncatedState.Index = compactLog.CompactIndex
			applySt.TruncatedState.Term = compactLog.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId), applySt)
			d.ScheduleCompactLog(applySt.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_ChangePeer:
		cc := &eraftpb.ConfChange{}
		_ = cc.Unmarshal(entry.Data)
		wb = d.processConfChange(entry, cc, wb)
	case raft_cmdpb.AdminCmdType_Split:
		d.processSplit(entry, msg, wb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
	}
	return wb
}

func (d *peerMsgHandler) processSplit(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	req := msg.AdminRequest
	split := req.Split
	region := d.Region()
	err := util.CheckKeyInRegion(split.SplitKey, region)
	if err != nil {
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return
	}
	if len(split.NewPeerIds) != len(region.Peers) {
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		})
		return
	}

	newPeers := make([]*metapb.Peer, 0)
	// sort
	for i := range split.NewPeerIds {
		newPeers = append(newPeers, &metapb.Peer{Id: req.Split.NewPeerIds[i], StoreId: d.Region().Peers[i].StoreId})
	}
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey:   region.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: InitEpochConfVer,
			Version: InitEpochConfVer,
		},
		Peers: newPeers,
	}

	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	//
	storeMeta.regionRanges.Delete(&regionItem{region: region})
	region.RegionEpoch.Version++
	region.EndKey = split.SplitKey

	storeMeta.setRegion(region, d.peer)
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})

	meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
	meta.WriteRegionState(wb, newRegion, rspb.PeerState_Normal)

	newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}
	for i := range newPeers {
		newPeer.insertPeerCache(newPeers[i])
	}
	storeMeta.setRegion(newRegion, newPeer)
	d.ctx.router.register(newPeer)
	d.ctx.router.send(split.NewRegionId, message.NewMsg(message.MsgTypeStart, nil))
	// for i := range storeMeta.pendingVotes {
	// 	votes := storeMeta.pendingVotes[i].ToPeer

	// }

	d.SizeDiffHint = 0
	d.ApproximateSize = nil
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		newPeer.MaybeCampaign(true)
	}
	d.notifyHeartbeatScheduler(newRegion, newPeer)

	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{region, newRegion},
				},
			},
		}
		p.cb.Done(resp)
	})
}
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

// processConfChange proposals[]
func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, confChange *eraftpb.ConfChange, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	data := &raft_cmdpb.RaftCmdRequest{}
	err := data.Unmarshal(confChange.Context)
	if err != nil {
		panic(err)
	}
	// ErrEpochNotMatch Checked before
	found := false
	pr := data.AdminRequest.ChangePeer.Peer
	for i := range d.Region().Peers {
		if d.Region().Peers[i].Id == pr.Id {
			found = true
			break
		}
	}
	switch confChange.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if found {
			break
		}
		d.Region().Peers = append(d.Region().Peers, pr)
		d.Region().RegionEpoch.ConfVer++
		meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
		d.insertPeerCache(pr)
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[d.regionId] = d.Region()
		storeMeta.Unlock()
		// if d.IsLeader() {
		// 	d.PeersStartPendingTime[confChange.NodeId] = time.Now()
		// }
	case eraftpb.ConfChangeType_RemoveNode:
		if !found {
			break
		}
		// if d.IsLeader() && len(d.Region().Peers) == 2 &&
		// 	d.Meta.Id == confChange.NodeId {
		// 	d.handleProposal(entry, func(p *proposal) {
		// 		p.cb.Done(ErrResp(errors.New("two node drop stale leader")))
		// 	})
		// 	return wb
		// }
		if d.Meta.Id == confChange.NodeId {
			if d.MaybeDestroy() {
				if d.IsLeader() {
					d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
				}
				d.destroyPeer()
			}
			// debuginfo return& break&handleProposal
			return wb
		}
		d.Region().RegionEpoch.ConfVer++
		for i := range d.Region().Peers {
			if d.Region().Peers[i].Id == pr.Id {
				d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
				break
			}
		}
		d.removePeerCache(confChange.NodeId)
		meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[d.regionId] = d.Region()
		storeMeta.Unlock()
	}
	// Apply to Raft/
	d.RaftGroup.ApplyConfChange(*confChange)
	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			},
		}
		p.cb.Done(resp)
	})
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return wb
}

func (d *peerMsgHandler) marshalEntry(entry *eraftpb.Entry) *raft_cmdpb.RaftCmdRequest {
	msg := new(raft_cmdpb.RaftCmdRequest)
	if entry.Data == nil {
		return nil
	}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		err = msg.Unmarshal(cc.Context)
		if err != nil {
			panic(err)
		}
	} else {
		err := msg.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
	}
	return msg
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	msg := d.marshalEntry(entry)
	if msg == nil {
		return wb
	}
	if msg.AdminRequest != nil {
		wb = d.processAdminRequest(entry, msg, wb)
		return wb
	}
	if len(msg.Requests) != 0 {
		d.processRequest(entry, msg, wb)
		return wb
	}
	return wb
}
func (d *peerMsgHandler) checkReadCommand(msg *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry) (bool, error) {
	readonly := true
	if msg.AdminRequest != nil {
		if msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer ||
			msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_CompactLog ||
			msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split ||
			msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
			readonly = false
		}
	}
	if msg.Requests != nil {
		req := msg.Requests
		for i := range req {
			if req[i].CmdType == raft_cmdpb.CmdType_Put || req[i].CmdType == raft_cmdpb.CmdType_Delete {
				readonly = false
				break
			}
		}
	}
	// errEpochNotMatching ErrRegionNotFound ErrStaleCommand
	if d.regionId != msg.Header.RegionId {
		if readonly {
			d.handleReadProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(&util.ErrRegionNotFound{RegionId: d.regionId}))
			})
		} else {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(&util.ErrRegionNotFound{RegionId: d.regionId}))
			})
		}
		return readonly, &util.ErrRegionNotFound{RegionId: d.regionId}
	}
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
		}
		if readonly {
			d.handleReadProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
		} else {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
		}
		return readonly, err
	}
	// if d.Term() > msg.Header.Term {
	// 	if readonly {
	// 		d.handleProposal(entry, func(p *proposal) {
	// 			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
	// 		})
	// 	} else {
	// 		d.handleProposal(entry, func(p *proposal) {
	// 			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
	// 		})
	// 	}
	// 	return readonly, err
	// }
	// util.ErrStoreNotMatch no need
	return readonly, nil
	// left CheckKeyInRegion && ErrNotLeader
}

func (d *peerMsgHandler) handleSnapshotReady(ready *raft.Ready, regionChange *ApplySnapResult) {
	// maybe need to Response readProposals(entry.Index<=appliedIdx)
	for i := range ready.CommittedEntries {
		entry := &ready.CommittedEntries[i]
		msg := d.marshalEntry(entry)
		if msg == nil {
			continue
		}
		readonly, err := d.checkReadCommand(msg, entry)
		if err != nil {
			continue
		}
		if readonly {
			d.handleReadProposal(entry, func(p *proposal) {
				NotifyStaleReq(d.Term(), p.cb)
			})
		} else {
			d.handleProposal(entry, func(p *proposal) {
				NotifyStaleReq(d.Term(), p.cb)
			})
		}
	}
	ready.CommittedEntries = nil
	wb := new(engine_util.WriteBatch)
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	wb.WriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) HandleReadEntries(entries []*eraftpb.Entry, handler func(*eraftpb.Entry, *raft_cmdpb.RaftCmdRequest)) {
	for i := range entries {
		entry := entries[i]
		if entry.Index > d.peerStorage.applyState.AppliedIndex {
			return
		}
		msg := d.marshalEntry(entry)
		if msg.AdminRequest != nil {
			if msg.AdminRequest.CmdType != raft_cmdpb.AdminCmdType_InvalidAdmin {
				log.Info("[warn] AdminCmdType_InvalidAdmin")
			}
			continue
		}
		if msg.Requests != nil {
			handler(entry, msg)
		}
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
	if d.RaftGroup.Raft.RaftLog.PendingSnapshot != nil && d.LastAppliedIdx != d.peerStorage.AppliedIndex() {
		return
	}
	ready := d.RaftGroup.Ready()
	regionChange, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(err)
	}
	randnum := rand.Intn(10000)
	log.Info("SaveReadyState finish", randnum)
	raftstate, err := meta.GetRaftLocalState(d.ctx.engine.Raft, d.regionId)
	if err != nil {
		log.Info(err)
	} else {
		log.Infof("RaftLocalState:lastindex=%v,committed=%v,term=%v,LastTerm=%v", raftstate.LastIndex, raftstate.HardState.Commit, raftstate.HardState.Term, raftstate.LastTerm)
	}
	// regionChange Has been apply SaveReadyState()(PeerStorage update)
	// Snapshot ConfChange change region
	if regionChange != nil {
		if !util.RegionEqual(regionChange.PrevRegion, regionChange.Region) {
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[regionChange.Region.Id] = regionChange.Region
			storeMeta.regionRanges.Delete(&regionItem{region: regionChange.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{regionChange.Region})
			storeMeta.Unlock()
			d.LastAppliedIdx = d.peerStorage.applyState.AppliedIndex
		}
		d.Send(d.ctx.trans, ready.Messages)
		d.handleSnapshotReady(&ready, regionChange)
		log.Info("advance_snapshot", randnum)
		raftstate, err = meta.GetRaftLocalState(d.ctx.engine.Raft, d.regionId)
		if err != nil {
			log.Info(err)
		} else {
			log.Infof("RaftLocalState:lastindex=%v,committed=%v,term=%v,LastTerm=%v", raftstate.LastIndex, raftstate.HardState.Commit, raftstate.HardState.Term, raftstate.LastTerm)
			log.Info("RaftPoint:!!", d.ctx.engine.Raft, " ", d.regionId)
		}
		d.RaftGroup.Advance(ready)
		return
	} else {
		if len(ready.CommittedEntries) != 0 {
			// should after process
			d.LastAppliedIdx = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		}
	}
	d.Send(d.ctx.trans, ready.Messages)
	if len(ready.CommittedEntries) != 0 {
		wb := &engine_util.WriteBatch{}
		readEntries := []*eraftpb.Entry{}
		for i := range ready.CommittedEntries {
			entry := &ready.CommittedEntries[i]
			// log.Infof("{DEBUG entry: %v string:%v %v}", entry.Data, entry.EntryType.String(), entry.Index)
			if entry.Index != d.peerStorage.applyState.AppliedIndex+1 {
				// TODO: maybe need panic ,not ErrStaleCommand callback
				panic(fmt.Sprintf("stale! entry.index:%v; appliedIndex:%v; LastAppliedIdx: %v", entry.Index, d.peerStorage.applyState.AppliedIndex, d.LastAppliedIdx))
			}
			msg := d.marshalEntry(entry)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			if msg == nil {
				continue
			}
			readonly, err := d.checkReadCommand(msg, entry)
			if err != nil {
				continue
			}
			if readonly {
				readEntries = append(readEntries, entry)
			} else {
				// log.Info("processWriteCmd")
				wb = d.process(&ready.CommittedEntries[i], wb)
			}
			if d.stopped {
				// TODO: need change to break
				break
			}
		}
		if !d.stopped {
			err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			err = wb.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic(err)
			}
		}
		if !d.stopped {
			d.HandleReadEntries(readEntries, d.handleReadCmd)
		} else {
			d.HandleReadEntries(readEntries, func(e *eraftpb.Entry, rcr *raft_cmdpb.RaftCmdRequest) {
				d.handleReadProposal(e, func(p *proposal) {
					p.cb.Done(nil)
				})
			})
		}
	}

	log.Info("advance", randnum)
	raftstate, err = meta.GetRaftLocalState(d.ctx.engine.Raft, d.regionId)
	if err != nil {
		log.Info(err)
	} else {
		log.Infof("RaftLocalState:lastindex=%v,committed=%v,term=%v,LastTerm=%v", raftstate.LastIndex, raftstate.HardState.Commit, raftstate.HardState.Term, raftstate.LastTerm)
		log.Info("RaftPoint:!!", d.ctx.engine.Raft, " ", d.regionId)
	}
	if d.stopped {
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		// d.destroyPeer()
		return
	}
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) handleReadCmd(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest) {
	for i := range msg.Requests {
		req := msg.Requests[i]
		if req.CmdType != raft_cmdpb.CmdType_Get && req.CmdType != raft_cmdpb.CmdType_Snap {
			panic("unexpected CmdType(Delete or Put) in handleReadCmd ")
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key := req.Get.Key
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				d.handleReadProposal(entry, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return
			}
		case raft_cmdpb.CmdType_Snap:
		}
	}
	d.handleReadProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		for i := range msg.Requests {
			req := msg.Requests[i]
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					val = nil
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: val,
					},
				})
			case raft_cmdpb.CmdType_Snap:
				log.Info("{Snap:%v}", d.regionId, d.Region())
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{
						Region: d.Region(),
					},
				})
				p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
		}
		p.cb.Done(resp)
	})
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

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
			cb.Done(ErrResp(errEpochNotMatching))
			return
		}
		cb.Done(ErrResp(err))
		return
	}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.applyState.GetAppliedIndex() {
			cb.Done(ErrResp(&util.ErrStaleCommand{}))
			break
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc := eraftpb.ConfChange{
			ChangeType: req.GetChangePeer().ChangeType,
			NodeId:     req.GetChangePeer().Peer.Id,
			Context:    data,
		}
		// if len(d.Region().Peers) == 2 && cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && d.PeerId() == cc.NodeId {
		// 	for i := range d.Region().Peers {
		// 		if d.Region().Peers[i].Id != cc.NodeId {
		// 			d.RaftGroup.TransferLeader(d.Region().Peers[i].Id)
		// 			break
		// 		}
		// 	}
		// }
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.RaftGroup.ProposeConfChange(cc)
		d.proposals = append(d.proposals, p)
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
	case raft_cmdpb.AdminCmdType_Split:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		err = util.CheckKeyInRegion(msg.GetAdminRequest().Split.SplitKey, d.peerStorage.region)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		nextIndex := d.nextProposalIndex()
		err = d.RaftGroup.Propose(data)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		p := &proposal{index: nextIndex, term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.GetPeer().GetId())
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		}
		cb.Done(resp)
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// maybe affect project2b
	if len(msg.Requests) == 0 {
		return
	}
	cmdType := msg.Requests[0].CmdType
	for i := range msg.Requests {
		if msg.Requests[i].CmdType != cmdType {
			log.Infof("[ERROR] RaftCmdRequest includes different types of operations.")
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	readOnly := true
	for i := range msg.Requests {
		key := make([]byte, 0)
		switch msg.Requests[i].CmdType {
		case raft_cmdpb.CmdType_Get:
			key = msg.Requests[i].Get.Key
		case raft_cmdpb.CmdType_Put:
			readOnly = false
			key = msg.Requests[i].Put.Key
		case raft_cmdpb.CmdType_Delete:
			readOnly = false
			key = msg.Requests[i].Delete.Key
		}
		if len(key) != 0 {
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}
	}
	err = util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
			cb.Done(ErrResp(errEpochNotMatching))
			return
		}
		cb.Done(ErrResp(err))
		return
	}

	nextIndex := d.nextProposalIndex()
	d.RaftGroup.Propose(data)
	proposal := &proposal{index: nextIndex, term: d.RaftGroup.Raft.Term, cb: cb}
	if !readOnly {
		// log.Infof("proposals:%v", proposal)
		d.proposals = append(d.proposals, proposal)
	} else {
		// log.Infof("readProposals:%v", proposal)
		d.readProposals = append(d.readProposals, proposal)
	}
}
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if d.stopped {
		return
	}
	if d.Region().Id != msg.Header.RegionId {
		cb.Done(ErrResp(&util.ErrRegionNotFound{
			RegionId: d.regionId,
		}))
		return
	}
	if msg.AdminRequest != nil {
		// log.Infof("[DEBUG]proposeRaftCommand Admin Side msg: %v callback: %v", msg, cb)
		d.proposeAdminRequest(msg, cb)
	}
	if len(msg.Requests) != 0 {
		// log.Infof("[DEBUG]proposeRaftCommand msg: %v callback: %v", msg, cb)
		d.proposeRequest(msg, cb)
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
	// log.Info("[DEBUG]_+_ onRaftBaseTick ")
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
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
	// log.Infof("[DEBUG]++ onRaftMsg call Step msg: %v", msg)
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
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
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
