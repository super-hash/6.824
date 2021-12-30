package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...) 创建新的raft server
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
	//	"bytes"

	"math/rand"
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type State string

const (
	Leader    State = "leader"
	Candidate State = "candidate"
	Follower  State = "follower"
)

type Entry struct {
	Term int
	//Command string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	//all server
	state       State
	currentTerm int
	votedFor    int
	logs        []Entry
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//return 300 --450ms
func RandomElectionTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	num := time.Duration(rand.Intn(151) + 300)

	return time.Duration(time.Millisecond * num)
}

//return 120ms
func StableHeartBeatTimer() time.Duration {

	return time.Duration(time.Millisecond * 120)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state==Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	IsHeartBeat  bool
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.state = Follower
	rf.electionTimer.Reset(RandomElectionTime())
	reply.Success = true

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) CallAppendEntries(server int, isHeartBeat bool) {
	
	args := AppendEntriesArgs{
		Term:        rf.currentTerm,
		LeaderId:    rf.me,
		IsHeartBeat: isHeartBeat,
	}
	reply := AppendEntriesReply{}
	if isHeartBeat {
		//DPrintf("%d leader send  HeartBeat  to %d in %d term", rf.me, server, rf.currentTerm)
		rf.sendAppendEntries(server, &args, &reply)
		if !reply.Success {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.electionTimer.Reset(RandomElectionTime())
		}
	} else {
		//DPrintf("%d leader send  entries to %d in %d term", rf.me, server, rf.currentTerm)
	}
}
func (rf *Raft) BroadcastHeartBeat(isHeartBeat bool) {

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.CallAppendEntries(server, isHeartBeat)
	}
}

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

// return true: rf 比参数server更新
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	len := len(rf.logs)
	if len == 0 {
		return false
	}
	if rf.logs[len-1].Term < lastLogTerm {
		return false
	}
	if rf.logs[len-1].Term == lastLogTerm && len-1 < lastLogIndex {
		return false
	}
	return true
}
func (rf *Raft) AttemptElection() {
	//DPrintf("%d  %v Attempt  election in %d term\n", rf.me,rf.state ,rf.currentTerm)
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	
	count := 1
	rf.votedFor = rf.me
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server,&args,&reply){
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//DPrintf("[%d] finish sending request vote  %d  %v in %d term", rf.me, server, reply.VoteGranted, rf.currentTerm)
				if rf.currentTerm == args.Term &&rf.state == Candidate{

					if reply.VoteGranted {
						count++
						if count > len(rf.peers)/2 {
							rf.state = Leader
							rf.heartBeatTimer.Reset(StableHeartBeatTimer())
							rf.BroadcastHeartBeat(true)
						}
					}else if reply.Term > rf.currentTerm{
						rf.state = Follower
						rf.electionTimer.Reset(RandomElectionTime())
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						//DPrintf("%d %v to Follower",rf.me,rf.state)

					}
				}
			}
		}(server)
	}
}
// func (rf *Raft) CallRequestVote(server int) bool {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	args := RequestVoteArgs{
// 		Term:        rf.currentTerm,
// 		CandidateId: rf.me,
// 	}
// 	reply := RequestVoteReply{}
// 	if rf.sendRequestVote(server, &args, &reply) {
// 		if reply.Term > rf.currentTerm {
// 			DPrintf("%d %v to Follower",rf.me,rf.state)
// 			rf.currentTerm = reply.Term
// 			rf.state = Follower
// 			rf.votedFor = -1
// 		}
// 		DPrintf("[%d] finish sending request vote  %d  %v in %d term", rf.me, server, reply.VoteGranted, rf.currentTerm)
// 		return reply.VoteGranted
// 	}
// 	DPrintf("%d %v  can`t sendRequestVote to  %d in %d term",rf.me,rf.state,server, rf.currentTerm)
// 	return false
// }

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <rf.currentTerm ||(args.Term == rf.currentTerm && rf.votedFor !=-1&&rf.votedFor != args.CandidateId){
		reply.Term,reply.VoteGranted = rf.currentTerm,false
		return
	}
	if args.Term>rf.currentTerm{
		rf.state = Follower
		rf.votedFor = -1
	}
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTime())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
//(   labrpc包模拟了一个有损耗的网络，在这个网络中，服务器可能是不可达的，请求和响应可能会丢失。Call()发送请求并等待应答。如果收到回复
// 在超时时间内，Call()返回true;否则Call()返回false。因此，Call()可能在一段时间内不会返回。
// 错误的返回可能是由于服务器死亡、无法访问的活动服务器、丢失的请求或丢失的应答。   )
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//DPrintf("[%d] %v killed by outer caller in %d term-----------------------------------------",rf.me,rf.state,rf.currentTerm)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			//DPrintf("%d %v Attempting election  in %d term \n", rf.me, rf.state, rf.currentTerm)
			rf.AttemptElection()
			rf.electionTimer.Reset(RandomElectionTime())
			rf.mu.Unlock()

		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				//DPrintf("%d %v Broading heartBeat to all server in %d term \n", rf.me, rf.state, rf.currentTerm)
				rf.BroadcastHeartBeat(true)
				rf.electionTimer.Reset(RandomElectionTime())
				rf.heartBeatTimer.Reset(StableHeartBeatTimer())  
			}
			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 服务或测试人员想要创建一个Raft服务器。所有的Raft服务器(包括本服务器)的端口都在peers[]。该服务器的端口是peers[me]。
//所有服务器的对等点[]数组具有相同的顺序。Persister是此服务器保存其持久状态的地方，并且最初还保存最近保存的状态(如果有的话)。
//applyCh是一个通道，
//测试者或服务希望Raft在该通道上发送ApplyMsg消息。Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		state:          Follower,
		votedFor:       -1,
		currentTerm:    0,
		commitIndex:    0,
		lastApplied:    0,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomElectionTime()),
		heartBeatTimer: time.NewTimer(StableHeartBeatTimer()),
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
