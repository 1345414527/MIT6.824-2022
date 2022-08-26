package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

func init() {
	rand.Seed(150)
}

type Role int

const (
	Role_Follower  = 0
	Role_Candidate = 1
	Role_Leader    = 2
)

const (
	ElectionTimeout   = time.Millisecond * 300 // 选举超时时间/心跳超时时间
	HeartBeatInterval = time.Millisecond * 150 // leader 发送心跳
	ApplyInterval     = time.Millisecond * 100 // apply log
	RPCTimeout        = time.Millisecond * 100
	MaxLockTime       = time.Millisecond * 10 // debug
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:接收到leader发来的快照，就要发送一条快照命令
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers，每一个clientEnd对应了向该peer通信的端点
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer
	applyCh             chan ApplyMsg //这个chan是用来提交应用的日志，具体的处理在config.go文件中
	notifyApplyCh       chan struct{}
	stopCh              chan struct{}

	lastSnapshotIndex int // 快照中最后一条日志的index，是真正的index，不是存储在logs中的index
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	flag := false
	if rf.role == Role_Leader {
		flag = true
	}
	return rf.currentTerm, flag
}

func (rf *Raft) getElectionTimeout() time.Duration {
	t := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	return t
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

//返回当前状态机的最后一条日志的任期和索引
//索引是一直会增大的，但是我们的日志队列却不可能无限增大，在队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.logs[len(rf.logs)-1].Term, rf.lastSnapshotIndex + len(rf.logs) - 1
}

func (rf *Raft) changeRole(newRole Role) {
	if newRole < 0 || newRole > 3 {
		panic("unknown role")
	}
	rf.role = newRole
	switch newRole {
	case Role_Follower:
	case Role_Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()
	case Role_Leader:
		//leader只有两个特殊的数据结构：nextIndex,matchIndex
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = lastLogIndex
		}
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}

}

//获取持久化的数据
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// 保存持久化状态
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
//读取持久化数据
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		currentTerm                                      int
		votedFor                                         int
		logs                                             []LogEntry
		commitIndex, lastSnapshotTerm, lastSnapshotIndex int
	)

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&logs) != nil {
		log.Fatal("rf read persist err!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logs = logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
//其实CondInstallSnapshot中的逻辑可以直接在InstallSnapshot中来完成，让CondInstallSnapshot成为一个空函数，这样可以减少锁的获取和释放
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	//installLen := lastIncludedIndex - rf.lastSnapshotIndex
	//if installLen >= len(rf.logs)-1 {
	//	rf.logs = make([]LogEntry, 1)
	//	rf.logs[0].Term = lastIncludedTerm
	//} else {
	//	rf.logs = rf.logs[installLen:]
	//}
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

	//其实接下来可以读入快照的数据进行同步，这里可以不写

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//生成一次快照，实现很简单，删除掉对应已经被压缩的 raft log 即可
//index是当前要压缩到的index，snapshot是已经帮我们压缩好的数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.lastSnapshotIndex
	if snapshotIndex >= index {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	oldLastSnapshotIndex := rf.lastSnapshotIndex
	rf.lastSnapshotTerm = rf.logs[rf.getStoreIndexByLogIndex(index)].Term
	rf.lastSnapshotIndex = index
	//删掉index前的所有日志
	rf.logs = rf.logs[index-oldLastSnapshotIndex:]
	//0位置就是快照命令
	rf.logs[0].Term = rf.lastSnapshotTerm
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	DPrintf("{Node %v}'s state is {role %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//客户端请求，leader添加日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Role_Leader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	_, lastIndex := rf.getLastLogTermAndIndex()
	index = lastIndex
	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1

	term = rf.currentTerm
	isLeader = true

	DPrintf("%v start command%v %v：%+v", rf.me, rf.lastSnapshotIndex, lastIndex, command)

	rf.resetAppendEntriesTimersZero()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//处理要应用的日志，快照的命令比较特殊，不在这里提交
func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)

	rf.mu.Lock()
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastSnapshotIndex {
		//此时要安装快照，命令在接收到快照时就发布过了，等待处理
		msgs = make([]ApplyMsg, 0)
		rf.mu.Unlock()
		rf.CondInstallSnapshot(rf.lastSnapshotTerm, rf.lastSnapshotIndex, rf.persister.snapshot)
		return
	} else if rf.commitIndex <= rf.lastApplied {
		// snapShot 没有更新 commitidx
		msgs = make([]ApplyMsg, 0)
	} else {
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getStoreIndexByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh: //当有日志记录提交了，要进行应用
				rf.startApplyLogs()
			}
		}
	}()

	//选举定时
	go func() {
		for rf.killed() == false {

			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}

		}
	}()

	//leader发送日志定时
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(cur int) {
			for rf.killed() == false {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[cur].C:
					rf.sendAppendEntriesToPeer(cur)
				}
			}
		}(i)
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("make a raft,me: %v", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Role_Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) //下标为0存储快照
	// initialize from state persisted before a crash
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//读取持久化数据
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatInterval)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.stopCh = make(chan struct{})

	// start ticker goroutine to start elections
	rf.ticker()

	return rf
}
