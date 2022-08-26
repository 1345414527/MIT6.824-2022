package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm || rf.role != Role_Follower {
		rf.changeRole(Role_Follower)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.resetElectionTimer()
		rf.persist()
	}

	//如果自身快照包含的最后一个日志>=leader快照包含的最后一个日志，就没必要接受了
	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	/********以下内容和CondInstallSnapshot的操作是相同的，因为不知道为什么在lab4B中只要调用CondInstallSnapshot函数就会陷入阻塞，因此将操作逻辑复制到这里一份，lab4中就没有调用CondInstallSnapshot函数了***********/

	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	_, lastIndex := rf.getLastLogTermAndIndex()
	if lastIncludedIndex > lastIndex {
		rf.logs = make([]LogEntry, 1)
	} else {
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.logs = rf.logs[installLen:]
		rf.logs[0].Command = nil
	}
	//0处是空日志，代表了快照日志的标记
	rf.logs[0].Term = lastIncludedTerm

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	/***********************************/

	//接收发来的快照，并提交一个命令处理
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

//向指定节点发送快照
func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()
	DPrintf("%v role: %v, send snapshot  to peer,%v,args = %+v,reply = %+v", rf.me, rf.role, server, args)

	for {
		timer.Stop()
		timer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		reply := &InstallSnapshotReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			DPrintf("%v role: %v, send snapshot to peer %v TIME OUT!!!", rf.me, rf.role, server)
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Role_Leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeRole(Role_Follower)
			rf.currentTerm = reply.Term
			rf.resetElectionTimer()
			rf.persist()
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		return
	}
}
