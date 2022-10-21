package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Result int

const (
	Fail Result = iota
	Success
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key         string
	Value       string
	OpType      string // "Put", "Append" or "Get"
	ExeResult   chan Result
	ClientId    int // client invoking request (6.3)
	SequenceNum int // to eliminate duplicates ($6.4)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // 这里的applyCh和raft.applyCh相同
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db             map[string]string
	maxSequenceNum map[int]int // 记录ClientId已执行命令的最大SequenceNum, 防止命令重复执行
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	/*
		Second, a leader must check whether it has been deposed before processing a read-only request (its
		information may be stale if a more recent leader has been elected). Raft handles this by having the leader
		exchange heartbeat messages with a majority of the cluster before responding to read-only requests.
	*/
	op := Op{OpType: "Get", ExeResult: make(chan Result, 1)}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	<-op.ExeResult
	reply.Err = OK

	kv.mu.Lock()
	reply.Value = kv.db[args.Key]
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value, OpType: args.Op, ExeResult: make(chan Result, 1),
		ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	<-op.ExeResult
	reply.Err = OK
}

func (kv *KVServer) executeCommand() {
	/*
		applyCh是tester或service期望Raft发送ApplyMsg消息的通道
		当每个Raftpeer意识到日志条目被提交时，peer应该通过传递给Make()的applyCh向同一服务器上的service（或tester）发送ApplyMsg
		applyCh中的附加日志已经处于committed状态, 需要在server中执行该附加日志中的指令
	*/
	for applyMsg := range kv.applyCh {
		command := applyMsg.Command
		op := command.(Op)
		kv.mu.Lock()
		switch op.OpType {
		case "Append":
			// 如果已经执行过, 那么不重复执行
			if kv.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			kv.db[op.Key] += op.Value
			kv.maxSequenceNum[op.ClientId] = op.SequenceNum
		case "Put":
			// 如果已经执行过, 那么不重复执行
			if kv.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			kv.db[op.Key] = op.Value
			kv.maxSequenceNum[op.ClientId] = op.SequenceNum
		default:
		}
		kv.mu.Unlock()
		op.ExeResult <- Success
	}
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
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
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
/**
 * @Description:k/v 服务器应该通过底层的 Raft 实现来存储快照，它应该调用 persister.SaveStateAndSnapshot() 来
 * 原子地保存 Raft 状态和快照。当 Raft 的保存状态超过 maxraftstate 字节时，k/v 服务器应该进行快照，以允许 Raft 对其日志进行
 * 垃圾收集。如果 maxraftstate 为 -1，则不需要快照。 StartKVServer() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
 * @param servers
 * @param me
 * @param persister
 * @param maxraftstate
 * @return *KVServer
 */
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.maxSequenceNum = make(map[int]int)
	go kv.executeCommand()

	return kv
}
