package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextLogTerm  int
	NextLogIndex int
}

//立马发送
func (rf *Raft) resetAppendEntriesTimersZero() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}

func (rf *Raft) resetAppendEntriesTimerZero(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(0)
}

//重置单个timer
func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

//判断当前raft的日志记录是否超过发送过来的日志记录
func (rf *Raft) isOutOfArgsAppendEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if lastLogTerm == args.Term && argsLastLogIndex < lastLogIndex {
		return true
	}
	return false
}

//获取当前存储位置的索引
func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}

//接收端处理rpc
//主要进行三个处理：
// 	1. 判断任期
// 	2. 判断是否接收数据，success：数据全部接受，或者根本就没有数据
//	3. 判断是否提交数据
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v receive a appendEntries: %+v", rf.me, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.getLastLogTermAndIndex()
	//先判断两边，再判断刚好从快照开始，再判断中间的情况
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		//1.要插入的前一个index小于快照index，几乎不会发生
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		//2. 要插入的前一个index大于最后一个log的index，说明中间还有log
		reply.Success = false
		reply.NextLogIndex = lastLogIndex + 1
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		//3. 要插入的前一个index刚好等于快照的index，说明可以全覆盖，但要判断是否是全覆盖
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0 //=0代表着插入会导致乱序
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else if args.PrevLogTerm == rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term {
		//4. 中间的情况：索引处的两个term相同
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:rf.getStoreIndexByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else {
		//5. 中间的情况：索引处的两个term不相同，跳过一个term
		term := rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && index > rf.lastSnapshotIndex && rf.logs[rf.getStoreIndexByLogIndex(index)].Term == term {
			index--
		}
		reply.Success = false
		reply.NextLogIndex = index + 1
	}

	if reply.Success {
		DPrintf("%v current commit: %v, try to commit %v", rf.me, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	DPrintf("%v role: %v, get appendentries finish,args = %v,reply = %+v", rf.me, rf.role, *args, *reply)
	rf.mu.Unlock()

}

//获取要向指定节点发送的日志
func (rf *Raft) getAppendLogs(peerId int) (prevLogIndex int, prevLogTerm int, logEntries []LogEntry) {
	nextIndex := rf.nextIndex[peerId]
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		//没有要发送的log
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
		return
	}
	//这里一定要进行深拷贝，不然会和Snapshot()发生数据上的冲突
	//logEntries = rf.logs[nextIndex-rf.lastSnapshotIndex:]
	logEntries = make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(logEntries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	prevLogIndex = nextIndex - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex].Term
	}

	return
}

//尝试去提交日志
//会依次判断，可以提交多个，但不能有间断
func (rf *Raft) tryCommitLog() {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	hasCommit := false

	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				//提交数达到多数派
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					DPrintf("%v role: %v,commit index %v", rf.me, rf.role, i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}

	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

//发送端发送数据
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		//尝试10次
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send append entries to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.role != Role_Leader {
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}
	DPrintf("%v send append entries to peer %v", rf.me, peerId)

	prevLogIndex, prevLogTerm, logEntries := rf.getAppendLogs(peerId)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()

	rf.sendAppendEntries(peerId, &args, &reply)

	DPrintf("%v role: %v, send append entries to peer finish,%v,args = %+v,reply = %+v", rf.me, rf.role, peerId, args, reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.changeRole(Role_Follower)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.role != Role_Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	//响应：成功了，即：发送的数据全部接收了，或者根本没有数据
	if reply.Success {
		if reply.NextLogIndex > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = reply.NextLogIndex
			rf.matchIndex[peerId] = reply.NextLogIndex - 1
		}
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			//每个leader只能提交自己任期的日志
			rf.tryCommitLog()
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}

	//响应：失败了，此时要修改nextIndex或者不做处理
	if reply.NextLogIndex != 0 {
		if reply.NextLogIndex > rf.lastSnapshotIndex {
			rf.nextIndex[peerId] = reply.NextLogIndex
			//为了一致性，立马发送
			rf.resetAppendEntriesTimerZero(peerId)
		} else {
			//发送快照
			go rf.sendInstallSnapshotToPeer(peerId)
		}
		rf.mu.Unlock()
		return
	} else {
		//reply.NextLogIndex = 0,此时如果插入会导致乱序，可以不进行处理
	}

	rf.mu.Unlock()
	return

}
