package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//默认失败，返回
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm == args.Term {
		if rf.role == Role_Leader {
			return
		}

		if args.CandidateId == rf.votedFor {
			reply.Term = args.Term
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && args.CandidateId != rf.votedFor {
			return
		}

		//还有一种情况，没有投过票
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.changeRole(Role_Follower)
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		rf.persist()
	}

	//判断日志完整性
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("%v， role：%v，voteFor: %v", rf.me, rf.role, rf.votedFor)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if server < 0 || server > len(rf.peers) || server == rf.me {
		panic("server invalid in sendRequestVote!")
	}

	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send request vote to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	if rf.role == Role_Leader {
		rf.mu.Unlock()
		return
	}

	rf.changeRole(Role_Candidate)
	DPrintf("%v role %v,start election,term: %v", rf.me, rf.role, rf.currentTerm)

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.persist()
	rf.mu.Unlock()

	allCount := len(rf.peers)
	grantedCount := 1
	resCount := 1
	grantedChan := make(chan bool, len(rf.peers)-1)
	for i := 0; i < allCount; i++ {
		if i == rf.me {
			continue
		}
		//对每一个其他节点都要发送rpc
		go func(gch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			gch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					//放弃选举
					rf.currentTerm = reply.Term
					rf.changeRole(Role_Follower)
					rf.votedFor = -1
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(grantedChan, i)

	}

	for rf.role == Role_Candidate {
		flag := <-grantedChan
		resCount++
		if flag {
			grantedCount++
		}
		DPrintf("vote: %v, allCount: %v, resCount: %v, grantedCount: %v", flag, allCount, resCount, grantedCount)

		if grantedCount > allCount/2 {
			//竞选成功
			rf.mu.Lock()
			DPrintf("before try change to leader,count:%d, args:%+v, currentTerm: %v, argsTerm: %v", grantedCount, args, rf.currentTerm, args.Term)
			if rf.role == Role_Candidate && rf.currentTerm == args.Term {
				rf.changeRole(Role_Leader)
			}
			if rf.role == Role_Leader {
				rf.resetAppendEntriesTimersZero()
			}
			rf.persist()
			rf.mu.Unlock()
			DPrintf("%v current role: %v", rf.me, rf.role)
		} else if resCount == allCount || resCount-grantedCount > allCount/2 {
			DPrintf("grant fail! grantedCount <= len/2:count:%d", grantedCount)
			return
		}
	}

}
