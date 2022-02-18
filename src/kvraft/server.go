package kvraft

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	Cid    int64
	SeqNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// timeout      time.Duration
	Persister *raft.Persister
	db        map[string]string
	chMap     map[int]chan Op
	cid2Seq   map[int64]int
	killCh    chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//可以通过Raft的Start函数返回请求在log中的index，对于每个index创建一个channel来接收执行完成的消息，
	//接收到了之后，知道是完成操作了，然后回复给client
	originOp := Op{
		OpType: "Get",
		Key:    args.Key,
		Value:  strconv.FormatInt(nrand(), 10),
		Cid:    0,
		SeqNum: 0,
	}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := beNotified(ch)
	if equalOp(op, originOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{
		args.Op,
		args.Key,
		args.Value,
		args.Cid,
		args.SeqNum,
	}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := beNotified(ch)
	if equalOp(originOp, op) {
		reply.WrongLeader = false
	}
}

//取到Op或超时返回
func beNotified(ch chan Op) Op {
	select {
	case notifyArg := <-ch:
		return notifyArg
	case <-time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

//对于每个index创建一个channel来接收执行完成的消息
func (kv *KVServer) putIfAbsent(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}
func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType && a.SeqNum == b.SeqNum && a.Cid == b.Cid
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.killCh <- true
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func send(notifyCh chan Op, op Op) {
	//select是执行选择操作的一个结构，它里面有一组case语句，它会执行其中无阻塞的那一个，如果都阻塞了，那就等待其中一个不阻塞，
	//进而继续执行，它有一个default语句，该语句是永远不会阻塞的，我们可以借助它实现无阻塞的操作。
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}
func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil {
		panic("readSnapShot ERROR for server ")
	} else {
		kv.db, kv.cid2Seq = db, cid2Seq
	}
}

func (kv *KVServer) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maxraftstate != -1 && kv.Persister.RaftStateSize() > kv.maxraftstate
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

//一个KVServer对应一个Raft
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.Persister = persister
	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)
	kv.readSnapShot(kv.Persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)
	// You may need initialization code here.
	//单独开一个goroutine来监视apply channel，一旦底层的Raft commit一个，就立马执行一个。
	go func() {
		for !kv.killed() {
			select {
			case <-kv.killCh:
				return
			case applyMsg := <-kv.applyCh:
				if applyMsg.CommandValid {
					op := applyMsg.Command.(Op)
					kv.mu.Lock()
					maxSeq, found := kv.cid2Seq[op.Cid]
					if !found || op.SeqNum > maxSeq {
						switch op.OpType {
						case "Put":
							kv.db[op.Key] = op.Value
						case "Append":
							kv.db[op.Key] += op.Value
						}
						kv.cid2Seq[op.Cid] = op.SeqNum //实现幂等性，保存Client处理的最大的任务序号（SeqNum
					}
					kv.mu.Unlock()
					//一旦底层的Raft commit一个，就立马执行一个。
					if kv.needSnapShot(){
						w := new(bytes.Buffer)
						e := labgob.NewEncoder(w)
						e.Encode(kv.db)
						e.Encode(kv.cid2Seq)
						kv.rf.Snapshot(applyMsg.CommandIndex, w.Bytes())
					}
					notifyCh := kv.putIfAbsent(applyMsg.CommandIndex)
					send(notifyCh, op)
				}
				index := 0
				if applyMsg.SnapshotValid && len(applyMsg.Snapshot) > 0 {
					if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
						r := bytes.NewBuffer(applyMsg.Snapshot)
						d := labgob.NewDecoder(r)
						d.Decode(&kv.db)
						d.Decode(&kv.cid2Seq)
						index = applyMsg.SnapshotIndex
					}
				}
				if index != 0 && kv.needSnapShot() {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.cid2Seq)
					kv.rf.Snapshot(index, w.Bytes())
				}

			}
		}
	}()
	return kv
}
