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
	"bytes"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	Index   int
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
	snapshot       []byte
	applyCond      *sync.Cond
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type LogType struct {
	Entries           []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (l *LogType) index(index int) Entry {
	return l.Entries[index-l.LastIncludedIndex]
}
func (rf *Raft) lastIndex() int {
	return len(rf.log.Entries) + rf.log.LastIncludedIndex - 1
}
func (rf *Raft) lastTerm() int {
	return rf.log.index(rf.lastIndex()).Term
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stopToFollower(args.Term)
		rf.persist()
	}
	rf.state = Follower
	rf.LeaderId = args.LeaderId
	rf.ResetElectionTimer()
	if args.LastIncludedIndex <= rf.commitIndex {
		//coming snapshot is older than our snapshot
		return
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
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
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.log.LastIncludedIndex {
		rf.log.Entries = make([]Entry, 1)
	} else {
		rf.log.Entries = append([]Entry(nil), rf.log.Entries[lastIncludedIndex-rf.log.LastIncludedIndex:]...)
		rf.log.Entries[0].Command = nil
	}
	rf.log.LastIncludedIndex = lastIncludedIndex
	rf.log.LastIncludedTerm = lastIncludedTerm
	rf.log.Entries[0].Term, rf.log.Entries[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.snapshot = snapshot
	rf.commitIndex = Max(rf.commitIndex, rf.log.LastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.log.LastIncludedIndex)
	rf.SaveStateAndSnapshot()
	return true
}
func (rf *Raft) CallInstallSnapshot(peerId int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peerId, &args, &reply)
	if !ok {
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stopToFollower(reply.Term)
		rf.persist()
		return
	}
	rf.mu.Lock()
	rf.nextIndex[peerId] = args.LastIncludedIndex + 1
	rf.matchIndex[peerId] = args.LastIncludedIndex
	rf.updateCommitIndex()
	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Signal()
	}
	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//加锁会产生死锁
	if index <= rf.log.LastIncludedIndex {
		return
	}
	rf.log.Entries = append([]Entry(nil), rf.log.Entries[index-rf.log.LastIncludedIndex:]...)
	rf.log.Entries[0].Command = nil
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = rf.log.index(index).Term
	rf.snapshot = snapshot
	rf.SaveStateAndSnapshot()
	rf.lastApplied = Max(rf.lastApplied, rf.log.LastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.log.LastIncludedIndex)
}
func (rf *Raft) SaveStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	snapshot := rf.snapshot
	rf.persister.SaveStateAndSnapshot(data, snapshot)
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
	return rf.currentTerm, rf.state == Leader
}

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
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex
	rf.mu.Unlock()
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
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.log.LastIncludedIndex { //#1
		reply.Term = rf.currentTerm
		return
	}
	rf.ResetElectionTimer()
	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.stopToFollower(args.Term)
	}
	rf.state = Follower
	rf.LeaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}
	if args.PrevLogIndex < rf.log.LastIncludedIndex {
		reply.Success = false
		return
	}
	//优化--Student Guide
	if args.PrevLogTerm != rf.log.index(args.PrevLogIndex).Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict Entries before.
		// bypass all Entries during the problematic term to speed up.
		reply.ConflictTerm = rf.log.index(args.PrevLogIndex).Term
		conflictIndex := args.PrevLogIndex - 1
		for conflictIndex >= rf.log.LastIncludedIndex && rf.log.index(conflictIndex).Term == reply.ConflictTerm {
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
				rf.log.Entries = rf.log.Entries[:logPos-rf.log.LastIncludedIndex] //3
				rf.log.Entries = append(rf.log.Entries, logEntry)                 //4
			}
		}
	}

	// firstIndex := rf.log.LastIncludedIndex
	// for index, entry := range args.Entries {
	// 	if entry.Index-firstIndex >= len(rf.log.Entries) || rf.log.Entries[entry.Index-firstIndex].Term != entry.Term {
	// 		rf.log.Entries = append([]Entry(nil), rf.log.Entries[:entry.Index-firstIndex]...)
	// 		rf.log.Entries = append(rf.log.Entries, args.Entries[index:]...)
	// 		break
	// 	}
	// }

	reply.Success = true
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastIndex())
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) updateCommitIndex() {
	for N := rf.lastIndex(); N > rf.commitIndex && rf.log.index(N).Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log.index(N).Term == rf.currentTerm {
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
	if args.PrevLogIndex+1 > rf.log.LastIncludedIndex {
		args.PrevLogTerm = rf.log.index(args.PrevLogIndex).Term
		args.Entries = append(args.Entries, rf.log.Entries[args.PrevLogIndex+1-rf.log.LastIncludedIndex:]...)
	}
	rf.mu.Unlock()

	go func() {
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if args.PrevLogIndex+1 <= rf.log.LastIncludedIndex {
			rf.CallInstallSnapshot(peerId)
			return
		}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
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
					rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
					rf.updateCommitIndex()
					if rf.commitIndex > rf.lastApplied {
						rf.applyCond.Signal()
					}
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
				tarIndex := reply.ConflictIndex //If it does not find an entry with that term
				if reply.ConflictTerm != -1 {
					logSize := rf.lastIndex() + 1                         //first search its log for conflictTerm
					for i := rf.log.LastIncludedIndex; i < logSize; i++ { //if it finds an entry in its log with that term,
						if rf.log.index(i).Term != reply.ConflictTerm {
							continue
						}
						for i < logSize && rf.log.index(i).Term == reply.ConflictTerm {
							i++
						} //set nextIndex to be the one
						tarIndex = i //beyond the index of the last entry in that term in its log
					}
				}
				rf.nextIndex[peerId] = Min(rf.lastIndex()+1, tarIndex)
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
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.stopToFollower(reply.Term)
					rf.persist()
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
	return rf.log.index(rf.lastIndex())
}
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	index := rf.lastIndex()
	term := rf.lastTerm()
	return lastLogTerm > term || (term == lastLogTerm && lastLogIndex >= index)
}

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
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	rf.ResetElectionTimer()
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.lastIndex() + 1
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   index,
	}
	rf.log.Entries = append(rf.log.Entries, entry)
	rf.persist()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
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
			rf.ResetElectionTimer()
			rf.AttemptElection()
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
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
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log.index(rf.lastApplied).Command,
				CommandIndex: rf.log.index(rf.lastApplied).Index,
				CommandTerm:  rf.log.index(rf.lastApplied).Term,
			}
		} else {
			rf.applyCond.Wait()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		votedFor:    -1,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomElectionTime()),
		heartBeatTimer: time.NewTimer(StableHeartBeatTime()),
		applyCh:        applyCh,
	}
	rf.log.Entries = make([]Entry, 1)
	rf.log.Entries[0].Term = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex
	go rf.ticker()
	go rf.applier()
	return rf
}
