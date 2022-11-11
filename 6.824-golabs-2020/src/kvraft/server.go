package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	ClientId    int    // client invoking request (6.3)
	SequenceNum int    // to eliminate duplicates ($6.4)
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
	indexToCh      sync.Map    // 日志索引index -> Op通道

	// 用于日志压缩
	lastIncludedIndex int // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int // term of lastIncludedIndex
}

func (kv *KVServer) start(comand Op) (isLeader bool) {
	index, _, isLeader := kv.rf.Start(comand)
	// rf.Start()会立即返回. 如果rf不是leader, 直接返回false
	if !isLeader {
		return false
	}

	val, ok := kv.indexToCh.Load(index)
	if !ok {
		/*
			如果index对应的通道不存在, 则新建通道
			这里选择缓冲通道/无缓冲通道对读取没有差别, 但是对写有差别. 选择缓冲通道的好处是, 写完以后不用等读取就可以继续执行
		*/
		kv.indexToCh.Store(index, make(chan Op, 1))
		val, _ = kv.indexToCh.Load(index)
	}
	ch := val.(chan Op)
	select {
	case op := <-ch:
		// index对应的Op通道只用一次
		kv.indexToCh.Delete(index)
		// 如果索引为index处对应的Op通道内命令的ClientId和SequenceNum与Client调用RPC时的不相同, 那么说明rf已经不是leader
		if op.ClientId != comand.ClientId || op.SequenceNum != comand.SequenceNum {
			return false
		}
	case <-time.After(800 * time.Millisecond):
		return false
	}
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	/*
			Second, a leader must check whether it has been deposed before processing a read-only request (its
			information may be stale if a more recent leader has been elected). Raft handles this by having the leader
			exchange heartbeat messages with a majority of the cluster before responding to read-only requests.
		这里的Get类型命令需要通道ExeResult是因为这里需要知道该命令是否已经被commit
	*/
	op := Op{Key: args.Key, OpType: "Get", ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	/*
		无法保证此命令将永远提交到Raft日志，因为leader可能会出故障或在选举中失败
		第一个返回值是该命令将出现的索引，如果它曾经被提交的话
		这里可能需要检查index和term是否一致
	*/
	isLeader := kv.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

	kv.mu.Lock()
	reply.Value = kv.db[args.Key]
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	isLeader := kv.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

//
// snapshot
//  @Description: 进行快照, 调用时不允许持有锁
//  @receiver kv
//  @return []byte 快照
//
func (kv *KVServer) snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.lastIncludedIndex)
	if err != nil {
		fmt.Printf("####Service%d快照时编码失败\n", kv.me)
	}
	err = e.Encode(kv.lastIncludedTerm)
	if err != nil {
		fmt.Printf("####Service%d快照时编码失败\n", kv.me)
	}
	err = e.Encode(kv.db)
	if err != nil {
		fmt.Printf("####Service%d快照时编码失败\n", kv.me)
	}
	err = e.Encode(kv.maxSequenceNum)
	if err != nil {
		fmt.Printf("####Service%d快照时编码失败\n", kv.me)
	}
	data := w.Bytes()
	return data
}

//
// installSnapshot
//  @Description: 安装快照, 调用时不允许持有锁
//  @receiver kv
//  @param data 快照
//
func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm int
	var db map[string]string
	var maxSequenceNum map[int]int
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&db) != nil || d.Decode(&maxSequenceNum) != nil {
		DPrintf("####decode error\n")
	} else {
		kv.lastIncludedIndex = lastIncludedIndex
		kv.lastIncludedTerm = lastIncludedTerm
		kv.db = db
		kv.maxSequenceNum = maxSequenceNum
	}
}

func (kv *KVServer) executeCommand() {
	/*
		applyCh是tester或service期望Raft发送ApplyMsg消息的通道
		当每个Raftpeer意识到日志条目被提交时，peer应该通过传递给Make()的applyCh向同一服务器上的service（或tester）发送ApplyMsg
		applyCh中的附加日志已经处于committed状态, 需要在server中执行该附加日志中的指令
	*/
	for applyMsg := range kv.applyCh {
		// 如果是快照
		if !applyMsg.CommandValid {
			snapshot := applyMsg.Command.([]byte)
			kv.installSnapshot(snapshot)
			continue
		}

		command := applyMsg.Command
		if command == nil {
			continue
		}
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
		case "Get":
			// 如果已经执行过, 那么不重复执行 这里需不需要这样存疑
			if kv.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			kv.maxSequenceNum[op.ClientId] = op.SequenceNum
		default:
			// 不需要有default
		}
		kv.lastIncludedIndex = applyMsg.CommandIndex
		kv.lastIncludedTerm = applyMsg.Term
		kv.mu.Unlock()
		/*
			对于相同ClientId和SequenceNum的Append/Put命令, 可能会有多个通道吗
			假设命令已经被执行, 但是由于网络延迟, 导致响应RPC没有及时到达, Client会重发请求, 这时已经不再需要重复执行命令,
			但是仍旧需要向通道发送Success消息, 否则会导致Get/PutAppend阻塞
		*/
		val, ok := kv.indexToCh.Load(applyMsg.CommandIndex)
		if !ok {
			/*
					如果index对应的通道不存在, 则新建通道
					这里选择缓冲通道/无缓冲通道对读取没有差别, 但是对写有差别. 选择缓冲通道的好处是, 写完以后不用等读取就可以继续执行
				    同一条命令可能会在日志里存储两次, 于是会有两个索引, 这两个索引对应的通道都需要发送消息
			*/
			kv.indexToCh.Store(applyMsg.CommandIndex, make(chan Op, 1))
			val, _ = kv.indexToCh.Load(applyMsg.CommandIndex)
		}
		ch := val.(chan Op)
		ch <- op
		close(ch)

		/*
			多久检查一次持久化状态的大小？
			注意只有达成共识，也就是被提交的日志才可能被快照。所以每次当有新的日志被提交时，就检查一次。如果持久化状态大小大于maxraftstate，则
			进行快照
		*/
		if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
			snapshot := kv.snapshot()
			kv.rf.Snapshot(snapshot)
		}
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
