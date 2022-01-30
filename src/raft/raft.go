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

	"bytes"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log Entries are
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
	CommandTerm  int

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
	Index	int
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//all server
	state          State
	currentTerm    int
	votedFor       int
	log            LogType
	nextIndex      []int
	matchIndex     []int
	commitIndex    int
	lastApplied    int
	LeaderId       int
	applyCh        chan ApplyMsg
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	snapshot	[]byte
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type LogType struct {
	Entries           []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}
func (l *LogType) index(index int) Entry{
	if index < l.LastIncludedIndex{
		panic("index < rf.log.LastIncludedIndex")
	}
	return l.Entries[index - l.LastIncludedIndex]
}
func (rf *Raft) lastIndex() int {
	return len(rf.log.Entries) + rf.log.LastIncludedIndex - 1
}
func (rf *Raft) lastTerm() int {
	return rf.GetLastEntry().Term
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.stopToFollower(args.Term)
	}
	rf.LeaderId = args.LeaderId
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex{
		return
	}
	rf.ResetElectionTimer()
	msg:= ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func(){
		rf.applyCh <- msg
	}()
}
//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.log.LastIncludedIndex{
		return false
	}
	defer func() {
		rf.log.LastIncludedIndex = lastIncludedIndex
		rf.log.LastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.commitIndex = lastIncludedIndex //IMPORTANT
		rf.lastApplied = lastIncludedIndex //IMPORTANT
		rf.SaveStateAndSnapshot()
	}()
	if lastIncludedIndex <= rf.lastIndex() && rf.log.index(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.Entries = append([]Entry(nil), rf.log.Entries[lastIncludedIndex-rf.log.LastIncludedIndex - 1:]...)
		return true
	}

	//discard the entire log
	rf.log.Entries = make([]Entry, 1)
	return true
}

//return 300 --450ms
func RandomElectionTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	num := time.Duration(rand.Intn(251) + 300)
	return time.Duration(time.Millisecond * num)
}

//return 120ms
func StableHeartBeatTime() time.Duration {
	return time.Duration(time.Millisecond * 100)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log LogType
	rf.mu.Lock()
	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.mu.Unlock()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.LastIncludedIndex{
		return
	}
	rf.log.Entries = append([]Entry(nil),rf.log.Entries[index-rf.log.LastIncludedIndex-1:]... )
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = rf.log.index(index).Term
	rf.snapshot = snapshot
	rf.SaveStateAndSnapshot()
}
func (rf *Raft) SaveStateAndSnapshot(){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	snapshot := rf.snapshot
	rf.persister.SaveStateAndSnapshot(data,snapshot)
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}
type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}
type InstallSnapshotReply struct {
	Term          int
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
	// rf.me, rf.LeaderId, args.Term, rf.currentTerm, rf.state, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Success = false
	reply.ConflictTerm = -1
	// defer func() {
	// 	DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] NextTryIndex[%d]",
	// 	rf.me, rf.LeaderId, args.Term, rf.currentTerm, rf.state, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log), reply.NextTryIndex)
	// }()

	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.log.LastIncludedIndex{ //#1
		reply.Term = rf.currentTerm
		return
	}
	rf.ResetElectionTimer()
	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.stopToFollower(args.Term)
	}
	rf.LeaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
		reply.ConflictIndex = rf.lastIndex() +  1
		return
	}

	// DPrintf("[%d] PrevLogIndex: %d",rf.me,args.PrevLogIndex)
	if args.PrevLogTerm != rf.log.index(args.PrevLogIndex).Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict Entries before.
		// bypass all Entries during the problematic term to speed up.
		reply.ConflictTerm = rf.log.index(args.PrevLogIndex).Term
		conflictIndex := args.PrevLogIndex
		for rf.log.index(conflictIndex-1).Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	//日志复制(只有在有冲突时才发生更改)
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := index
		if index > rf.lastIndex() {
			rf.log.Entries = append(rf.log.Entries, logEntry)
		} else {
			if rf.log.index(logPos).Term != logEntry.Term {
				rf.log.Entries = rf.log.Entries[:logPos - rf.log.LastIncludedIndex]          //3
				rf.log.Entries = append(rf.log.Entries, logEntry) //4
			}
		}
	}

	reply.Success = true
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastIndex())
		go rf.applier()
	}

}
func (rf *Raft) stopToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.LeaderId = -1
	rf.heartBeatTimer.Stop()
	rf.ResetElectionTimer()
}
func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) updateCommitIndex() {
	for N := rf.lastIndex(); N > rf.commitIndex && rf.log.Entries[N].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log.Entries[N].Term == rf.currentTerm {
			rf.commitIndex = N
			break
		}
	}
}
func (rf *Raft) CallAppendEntries(peerId int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]Entry, 0)
	args.PrevLogIndex = rf.nextIndex[peerId] - 1
	args.PrevLogTerm = rf.log.Entries[args.PrevLogIndex].Term
	args.Entries = append(args.Entries, rf.log.Entries[args.PrevLogIndex+1:]...)
	// DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
	// rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)
	rf.mu.Unlock()

	go func() {
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		if args.PrevLogIndex+1 <= rf.log.LastIncludedIndex{
			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LastIncludedIndex: rf.log.LastIncludedIndex,
				LastIncludedTerm: rf.log.LastIncludedTerm,
				Data: rf.snapshot,
			}
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(peerId,&args,&reply)
		}
		rf.mu.Unlock()
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
				rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stopToFollower(reply.Term)
				rf.persist()
				return
			}

			if reply.Success { // 同步日志成功
				if len(args.Entries) > 0 {
					rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1 - rf.log.LastIncludedIndex
					rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)- rf.log.LastIncludedIndex
					rf.updateCommitIndex()
					go rf.applier()
				}
			} else { //加速日志回溯（https://thesquareplanet.com/blog/students-guide-to-raft/）
				/*
					1.优化以后，理想情况是一个 RPC 能够至少检验一个 Term 的 log。
					2.Follower 在 prevLogIndex 处发现不匹配，设置好 ConflictTerm，同时 ConflictIndex
					被设置为这个 ConflictTerm 的 第一个 log entry。如果 Leader 中不存在 ConflictTerm ,
					则会使用 ConflictIndex 来跳过这整个 ConflictTerm 的所有 log。

					3.如果 Follower 返回的 ConflictTerm 在 Leader 的 log 中找不到，
					说明这个 ConflictTerm 不会存在需要 replication 的 log。 对下一个 RPC 中的 prevLogIndex
					的最好的猜测就是将 nextIndex 设置为 ConflictIndex，直接跳过 ConflictIndex。

					4.如果 Follower 返回的 ConflictTerm 在 Leader 的 log 中找到， 说明我们还需要 replicate ConflictTerm 的某些 log，
					此时就不能使用 ConflictIndex 跳过， 而是将 nextIndex 设置为 Leader 属于 ConflictTerm 的 log 之后的 第一个 log，
					 这样使下一轮 prevLogIndex 能够从正确的 log 开始。
				*/
				rf.nextIndex[peerId] = reply.ConflictIndex - rf.log.LastIncludedIndex
				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex- rf.log.LastIncludedIndex; i >= 1; i-- {
						if rf.log.index(i-1).Term == reply.ConflictTerm {
							rf.nextIndex[peerId] = i 
							break
						}
					}
				}
			}
		}
	}()

}
func (rf *Raft) BroadcastHeartBeat() {

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.CallAppendEntries(server)
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

func (rf *Raft) AttemptElection() {
	//DPrintf("%d  %v Attempt  election in %d term\n", rf.me,rf.state ,rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}

	count := 1
	rf.votedFor = rf.me
	rf.persist()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf("[%d] finish sending request vote  %d  %v in %d term", rf.me, server, reply.VoteGranted, rf.currentTerm)

				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.stopToFollower(reply.Term)
					rf.persist()
					// DPrintf("%d %v to Follower", rf.me, rf.state)
				}
				if reply.VoteGranted {
					count++
					if count > len(rf.peers)/2 {
						rf.state = Leader
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.lastIndex() + 1
						}
						for i := 0; i < len(rf.peers); i++ {
							rf.matchIndex[i] = 0
						}
						rf.electionTimer.Stop()
						rf.ResetHeartBeatTimer()
						rf.BroadcastHeartBeat()
					}
				}

			}

		}(server)
	}
}
func (rf *Raft) ResetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(RandomElectionTime())
}
func (rf *Raft) ResetHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(StableHeartBeatTime())
}
func (rf *Raft) GetLastEntry() Entry {
	return rf.log.Entries[rf.lastIndex()]
}
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	index := rf.lastIndex()
	term := rf.lastTerm()
	return lastLogTerm > term || (term == lastLogTerm && lastLogIndex >= index)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.stopToFollower(args.Term)
	}
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("islog up date  [%d]is new (lastindex: %d , lastTerm: %d) but [%d] is old (lastindex: %d , lastTerm: %d)",rf.me,rf.lastIndex(),rf.lastTerm(),args.CandidateId,args.LastLogIndex,args.LastLogTerm)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	//DPrintf("[%d]  rf.votedFor : %d",rf.me,rf.votedFor)
	rf.ResetElectionTimer()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// Your code here (2B).
	
	index := rf.lastIndex()+1
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index: index,
	}
	rf.log.Entries = append(rf.log.Entries, entry)
	rf.persist()
	DPrintf("{Node %v} receives a new command[%v]is {%v} to replicate in term %v", rf.me, index, entry,rf.currentTerm)
	return index, rf.currentTerm, true
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
	// DPrintf("[%d] %v killed by outer caller in %d term-----------------------------------------",rf.me,rf.state,rf.currentTerm)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			// DPrintf("%d %v Attempting election  in %d term \n", rf.me, rf.state, rf.currentTerm)
			rf.ResetElectionTimer()
			rf.AttemptElection()
			rf.mu.Unlock()

		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				// DPrintf("%d %v Broading heartBeat to all server in %d term \n", rf.me, rf.state, rf.currentTerm)
				rf.BroadcastHeartBeat()
				rf.ResetHeartBeatTimer()
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) applier() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log.index(i).Command,
			CommandIndex: i,
			CommandTerm: rf.log.index(i).Term,
		}
		rf.lastApplied = i
		DPrintf("........RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
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
		
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomElectionTime()),
		heartBeatTimer: time.NewTimer(StableHeartBeatTime()),
		applyCh:        applyCh,
	}
	rf.log.Entries = make([]Entry,1)
	rf.log.LastIncludedIndex = 0
	rf.log.LastIncludedTerm = 0
	rf.log.Entries[0].Term = 0
	rf.lastApplied = rf.log.LastIncludedIndex

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
