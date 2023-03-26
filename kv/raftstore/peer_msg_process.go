package raftstore

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

func (d *peerMsgHandler) processAdmin(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kv *engine_util.WriteBatch) {
	defer kv.WriteToDB(d.peerStorage.Engines.Kv)
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLogRequest := req.GetCompactLog()
		if d.peer.peerStorage.applyState.TruncatedState.Index >= compactLogRequest.CompactIndex {
			return
		}
		d.peer.peerStorage.applyState.TruncatedState.Index = compactLogRequest.CompactIndex
		d.peer.peerStorage.applyState.TruncatedState.Term = compactLogRequest.CompactTerm
		if err := kv.SetMeta(meta.ApplyStateKey(d.regionId), d.peer.peerStorage.applyState); err != nil {
			panic(err)
		}
		d.ScheduleCompactLog(compactLogRequest.CompactIndex)
	case raft_cmdpb.AdminCmdType_Split:
		d.processAdminSplit(entry, msg, kv)
	}
}

func (d *peerMsgHandler) processAdminSplit(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kv *engine_util.WriteBatch) {
	req := msg.AdminRequest

	var (
		split                          = req.Split
		ids                            = split.NewPeerIds
		newPeerEndKey, newPeerStartKey []byte
		splitKey                       = split.SplitKey
		oldRegion                      = d.Region()
	)
	if err := util.CheckKeyInRegion(splitKey, oldRegion); err != nil {
		d.handleProposal(*entry, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return
	}
	newPeers := make([]*metapb.Peer, 0)
	// the len of peers should be equal
	if len(ids) != len(oldRegion.Peers) {
		return
	}

	for i, p := range oldRegion.Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      msg.AdminRequest.Split.NewPeerIds[i],
			StoreId: p.StoreId,
		})
	}

	if bytes.Compare(splitKey, oldRegion.StartKey) == 0 || bytes.Compare(splitKey, oldRegion.EndKey) == 0 {
		return
	}

	// judge the endKey's range
	if engine_util.ExceedEndKey(splitKey, oldRegion.EndKey) {
		newPeerStartKey = oldRegion.EndKey
		newPeerEndKey = splitKey
	} else {
		newPeerStartKey = splitKey
		newPeerEndKey = oldRegion.EndKey
		oldRegion.EndKey = splitKey
	}

	oldRegion.RegionEpoch.Version++
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: newPeerStartKey,
		EndKey:   newPeerEndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version,
		},
		Peers: newPeers,
	}
	m := d.ctx.storeMeta
	{
		m.Lock()
		m.regionRanges.ReplaceOrInsert(&regionItem{oldRegion})
		m.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
		m.setRegion(oldRegion, d.peer)
		m.regions[newRegion.Id] = newRegion
		m.Unlock()
	}

	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}
	// put all the peers into chache
	for _, p := range newRegion.Peers {
		newPeer.insertPeerCache(p)
	}

	// register a new peer and start it
	{
		d.ctx.router.register(newPeer)
		d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
	}
	// write the kv to storage
	{
		meta.WriteRegionState(kv, oldRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kv, newRegion, rspb.PeerState_Normal)
	}

	{
		d.notifyHeartbeatScheduler(d.peer, oldRegion)
		d.notifyHeartbeatScheduler(newPeer, newRegion)
	}

	d.handleProposal(*entry, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{oldRegion, newRegion},
				},
			},
		})
	})
}

func (d *peerMsgHandler) processResp(req *raft_cmdpb.Request) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
	}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
		if err != nil {
			val = nil
		}
		resp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     &raft_cmdpb.GetResponse{Value: val},
		}}
	case raft_cmdpb.CmdType_Put:
		resp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		}}
	case raft_cmdpb.CmdType_Delete:
		resp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		}}
	case raft_cmdpb.CmdType_Snap:
		resp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
		}}
	}
	return resp
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		_, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		d.Send(d.ctx.trans, rd.Messages)
		if !raft.IsEmptySnap(&rd.Snapshot) {
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.regions[d.regionId] = d.peerStorage.region
		}
		for _, entry := range rd.CommittedEntries {
			if d.stopped {
				return
			}
			kvDB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err = kvDB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			if entry.EntryType == eraftpb.EntryType_EntryConfChange {
				confChange := &eraftpb.ConfChange{}
				if err := confChange.Unmarshal(entry.Data); err != nil {
					panic(err)
				}
				d.processConfChange(&entry, confChange, kvDB)
			} else {
				msg := &raft_cmdpb.RaftCmdRequest{}
				if err := msg.Unmarshal(entry.Data); err != nil {
					panic(err)
				}
				if msg.AdminRequest != nil {
					d.processAdmin(&entry, msg, kvDB)
				} else {
					d.processNormalRequests(&entry, msg, kvDB)
				}
			}
		}
		d.RaftGroup.Advance(rd)
	}
}

func regionPeerIndex(region *metapb.Region, id uint64, storeId uint64) (int, int) {
	indexId := -1
	indexStore := -1
	for i, peer := range region.Peers {
		if peer.Id == id {
			indexId = i
		}
		if peer.StoreId == storeId {
			indexStore = i
		}
	}
	return indexId, indexStore
}

func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	add := cc.ChangeType == eraftpb.ConfChangeType_AddNode
	newPeer := msg.AdminRequest.ChangePeer.Peer
	idIndex, storeIndex := regionPeerIndex(region, cc.NodeId, newPeer.StoreId)
	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		if idIndex != -1 || storeIndex != -1 {
			goto handle
		}
	}
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		if idIndex == -1 || storeIndex == -1 {
			goto handle
		}
	}
	// newPeer.StoreId
	if add {
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		region.Peers = append(region.Peers, newPeer)
		d.insertPeerCache(newPeer)
		region.RegionEpoch.ConfVer++
		{
			d.ctx.storeMeta.regions[d.regionId] = region
			d.SetRegion(region)
		}
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
			panic(err)
		}
		storeMeta.Unlock()
	} else {
		if d.PeerId() == cc.NodeId {
			// d.SetRegion(region)
			if len(d.Region().Peers) == 2 && d.IsLeader() {
				var targetPeer uint64 = 0
				for _, peer := range d.Region().Peers {
					if peer.Id != d.PeerId() {
						targetPeer = peer.Id
						break
					}
				}
				d.RaftGroup.TransferLeader(targetPeer)
			}
			// region.RegionEpoch.ConfVer++
			d.destroyPeer()
		} else {
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			d.peer.removePeerCache(cc.NodeId)
			util.RemovePeer(region, cc.NodeId)
			region.RegionEpoch.ConfVer++
			{
				d.ctx.storeMeta.regions[d.regionId] = region
				d.SetRegion(region)
			}
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
				panic(err)
			}
			storeMeta.Unlock()
		}
	}
handle:
	{
		d.RaftGroup.ApplyConfChange(*cc)
		//if d.IsLeader() {
		//    d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		//}
		d.notifyHeartbeatScheduler(d.peer, region)
	}

	// handle the proposals
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		for p.index < entry.Index {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			if len(d.proposals) != 0 {
				p = d.proposals[0]
				continue
			} else {
				break
			}
		}
		if p.index == entry.Index {
			d.proposals = d.proposals[1:]
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				response := &raft_cmdpb.RaftCmdResponse{
					Header:        &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
					AdminResponse: &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()}},
				}
				p.cb.Done(response)
			}
		}
	}
}

func getRequestedKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	}
	return nil
}

func (d *peerMsgHandler) processNormalRequests(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	requests := msg.Requests
	for _, request := range requests {
		key := getRequestedKey(request)
		if key != nil {
			// check the region
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				d.handleProposal(*entry, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return
			}
		}
		switch request.CmdType {
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(request.Delete.Cf,
				request.Delete.Key)
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(request.Put.Cf,
				request.Put.Key,
				request.Put.Value)
		}
	}

	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index < entry.Index {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			if len(d.proposals) != 0 {
				continue
			} else {
				break
			}
		} else {
			break
		}
	}

	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			d.proposals = d.proposals[1:]
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
					p.cb.Done(ErrResp(err))
					kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
					return
				}
				response := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()}, Responses: []*raft_cmdpb.Response{}}

				d.handleRequest(requests, response, p)

				err := kvWB.WriteToDB(d.peerStorage.Engines.Kv)
				if err != nil {
					p.cb.Done(ErrResp(err))
				} else {
					p.cb.Done(response)
				}
				return
			}
		}
	}
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) handleRequest(requests []*raft_cmdpb.Request, response *raft_cmdpb.RaftCmdResponse, p *proposal) {
	for _, request := range requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv,
				request.Get.Cf, request.Get.Key)
			if err != nil {
				response.Header.Error = util.RaftstoreErrToPbError(err)
			}
			response.Responses = append(response.Responses,
				&raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: value},
				})
		case raft_cmdpb.CmdType_Put:
			response.Responses = append(response.Responses,
				&raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
		case raft_cmdpb.CmdType_Delete:
			response.Responses = append(response.Responses,
				&raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})

		case raft_cmdpb.CmdType_Snap:
			region := &metapb.Region{
				Id:          d.Region().Id,
				StartKey:    d.Region().StartKey,
				EndKey:      d.Region().EndKey,
				RegionEpoch: d.Region().RegionEpoch,
				Peers:       d.Region().Peers,
			}
			response.Responses = append(response.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: region},
			})
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(peer *peer, region *metapb.Region) {
	clonedRegion := new(metapb.Region)
	if err := util.CloneMsg(region, clonedRegion); err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		ApproximateSize: peer.ApproximateSize,
		Region:          clonedRegion,
		PendingPeers:    peer.CollectPendingPeers(),
		Peer:            peer.Meta,
	}
}

func (d *peerMsgHandler) handleProposal(entry eraftpb.Entry, handler func(proposal2 *proposal)) {
	if len(d.proposals) == 0 {
		return
	}
	p := d.proposals[0]
	if p.index == entry.Index {
		if p.term != entry.Term {
			NotifyStaleReq(entry.Term, p.cb)
		} else {
			handler(p)
		}
	}
	d.proposals = d.proposals[1:]
}
