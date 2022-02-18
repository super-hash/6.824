package kvraft

import (
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
	Persister    *raft.Persister
	db           map[string]string
	chMap        map[int]chan Op
	cid2Seq map[int64]int
    killCh  chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
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
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}
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
func beNotified(ch chan Op) Op {
	select {
	case notifyArg := <-ch:
		return notifyArg
	case <-time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}
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
func send(notifyCh chan Op,op Op) {
    select{
    case  <-notifyCh:
    default:
    }
    notifyCh <- op
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

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool,1)
	// You may need initialization code here.
	go func(){
		for !kv.killed(){
			select{
			case <-kv.killCh:
				return
			case applyMsg:=<- kv.applyCh:
				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				maxSeq,found := kv.cid2Seq[op.Cid]
				if !found || op.SeqNum>maxSeq{
					switch op.OpType{
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.cid2Seq[op.Cid] = op.SeqNum
				}
				kv.mu.Unlock()
				notifyCh := kv.putIfAbsent(applyMsg.CommandIndex)
				send(notifyCh,op)
			}
		}
	}()
	return kv
}
