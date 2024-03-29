package shardmaster

import (
	"../raft"
	"log"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxSequenceNum map[int]int // 记录ClientId已执行命令的最大SequenceNum, 防止命令重复执行
	indexToCh      sync.Map    // 日志索引index -> Op通道
	configs        []Config    // indexed by config num
}

type Op struct {
	// Your data here.
	Args        interface{}
	OpType      string // "Join", "Leave", "Move" or "Query"
	ClientId    int    // client invoking request
	SequenceNum int    // to eliminate duplicates
}

type void struct{}

var member void

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) start(comand Op) (isLeader bool) {
	index, _, isLeader := sm.rf.Start(comand)
	// rf.Start()会立即返回. 如果rf不是leader, 直接返回false
	if !isLeader {
		return false
	}

	val, ok := sm.indexToCh.Load(index)
	if !ok {
		/*
			如果index对应的通道不存在, 则新建通道
			这里选择缓冲通道/无缓冲通道对读取没有差别, 但是对写有差别. 选择缓冲通道的好处是, 写完以后不用等读取就可以继续执行
		*/
		sm.indexToCh.Store(index, make(chan Op, 1))
		val, _ = sm.indexToCh.Load(index)
	}
	ch := val.(chan Op)
	select {
	case op := <-ch:
		// index对应的Op通道只用一次
		sm.indexToCh.Delete(index)
		// 如果索引为index处对应的Op通道内命令的ClientId和SequenceNum与Client调用RPC时的不相同, 那么说明rf已经不是leader
		if op.ClientId != comand.ClientId || op.SequenceNum != comand.SequenceNum {
			return false
		}
	case <-time.After(800 * time.Millisecond):
		return false
	}
	return true
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Args: *args, OpType: "Join", ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	isLeader := sm.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Args: *args, OpType: "Leave", ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	isLeader := sm.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Args: *args, OpType: "Move", ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	isLeader := sm.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Args: *args, OpType: "Query", ClientId: args.ClientId, SequenceNum: args.SequenceNum}
	isLeader := sm.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = OK

	sm.mu.Lock()
	reply.Config = sm.query(*args)
	sm.mu.Unlock()
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//  @Description: 调用时必须持有锁
//  @receiver sm
//  @param servers new GID -> servers mappings
//
func (sm *ShardMaster) join(args JoinArgs) {
	config := sm.configs[len(sm.configs)-1].Copy()
	DPrintf("####Server%djoin前args.Servers%+v\n", sm.me, args.Servers)
	DPrintf("####Server%djoin前config.Num%d#config.Shards%+v\n", sm.me, config.Num, config.Shards)
	config.Num = len(sm.configs)
	// add "gid -> servers[]" key value pairs
	for k, v := range args.Servers {
		config.Groups[k] = append([]string{}, v...)
	}

	/*
		注意原来的config.Groups就可能因为Move操作而负载不均衡, 所以不属于args.Servers的gid也要重新负载均衡
		负载不均衡既可能由Join和Leave操作导致, 也可能由Move操作导致
	*/
	sm.adjustConfig(&config)
	DPrintf("####Server%djoin后config.Num%d#config.Shards%+v\n", sm.me, config.Num, config.Shards)
	sm.configs = append(sm.configs, config)
}

//  @Description: 调整配置
//  时间复杂度为O(NShards)的负载均衡算法
//  @receiver sm
//  @param config 未经调整的配置
//  @return Config 调整后的配置
//
func (sm *ShardMaster) adjustConfig(config *Config) {
	// 如果Replica Group变为空, 将Shard都分配给gid0
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
		return
	} else if len(config.Groups) >= NShards {
		// 此时每个gid最多负载一个Shard
		sm.oneShardPerGID(config)
		return
	}

	// 每个gid的平均负载
	avg := NShards / len(config.Groups)
	// 需要被重新分配的Shard
	unallocShard := make(map[int]void)
	// 最终配置里, 有plusOneCnt个gid的负载为avg+1, 其它gid的负载都为avg
	plusOneCnt := NShards % len(config.Groups)
	// 负载为avg+1的gid Set
	plusOneGID := make(map[int]void)
	// 当前负载Shard数量低于avg的gid -> 负载
	gidToUnderLoad := make(map[int]int)
	// gid -> 当前负载Shard数量
	gidToLoad := make(map[int]int)

	// 初始化gidToLoad
	for gid, _ := range config.Groups {
		gidToLoad[gid] = 0
	}
	for i := 0; i < NShards; i++ {
		_, ok := gidToLoad[config.Shards[i]]
		if !ok || gidToLoad[config.Shards[i]] == avg+1 {
			unallocShard[i] = member
		} else if gidToLoad[config.Shards[i]] == avg {
			_, ok := plusOneGID[config.Shards[i]]
			if !ok && len(plusOneGID) < plusOneCnt {
				plusOneGID[config.Shards[i]] = member
				gidToLoad[config.Shards[i]]++
			} else {
				unallocShard[i] = member
			}
		} else {
			gidToLoad[config.Shards[i]]++
		}
	}

	// 初始化gidToUnderLoad
	for gid, load := range gidToLoad {
		if load < avg {
			gidToUnderLoad[gid] = load
		}
	}

	for gid, _ := range gidToUnderLoad {
		for shard, _ := range unallocShard {
			config.Shards[shard] = gid
			gidToLoad[gid]++
			gidToUnderLoad[gid]++
			delete(unallocShard, shard)
			if gidToUnderLoad[gid] == avg {
				delete(gidToUnderLoad, gid)
				break
			}
		}
	}

	// 如果仍旧有需要被重新分配的Shard, 那么分配给负载为avg的gid
	if len(unallocShard) != 0 {
		// 负载为avg的gid Set
		loadAvgGID := make(map[int]void)
		for gid, load := range gidToLoad {
			if load == avg {
				loadAvgGID[gid] = member
			}
		}

		for shard, _ := range unallocShard {
			for gid, _ := range loadAvgGID {
				config.Shards[shard] = gid
				delete(loadAvgGID, gid)
				delete(unallocShard, shard)
			}
		}
	}
}

//  @Description: 每个gid最多负载一个Shard的负载均衡算法
//  @receiver sm
//  @param config
//
func (sm *ShardMaster) oneShardPerGID(config *Config) {
	// 需要被重新分配的Shard
	unallocShard := make(map[int]void)
	// 已经被分配一个Shard的gid
	allocGID := make(map[int]void)
	for i := 0; i < NShards; i++ {
		if _, ok := config.Groups[config.Shards[i]]; !ok {
			unallocShard[i] = member
		} else if _, ok := allocGID[config.Shards[i]]; ok {
			unallocShard[i] = member
		} else {
			allocGID[config.Shards[i]] = member
		}
	}

	for gid, _ := range config.Groups {
		if len(unallocShard) == 0 {
			break
		}
		if _, ok := allocGID[gid]; ok {
			continue
		}
		for shard, _ := range unallocShard {
			config.Shards[shard] = gid
			delete(unallocShard, shard)
			allocGID[gid] = member
			break
		}
	}
}

//  @Description: 调用时必须持有锁
//  @receiver sm
//  @param args
//
func (sm *ShardMaster) leave(args LeaveArgs) {
	config := sm.configs[len(sm.configs)-1].Copy()
	DPrintf("####Server%dleave前args.GIDs%+v\n", sm.me, args.GIDs)
	DPrintf("####Server%dleave前config.Num%d#config.Shards%+v\n", sm.me, config.Num, config.Shards)
	config.Num = len(sm.configs)
	// delete "gid -> servers[]" key value pairs
	for _, v := range args.GIDs {
		delete(config.Groups, v)
	}

	/*
		注意原来的config.Groups就可能因为Move操作而负载不均衡, 所以不属于args.Servers的gid也要重新负载均衡
		负载不均衡既可能由Join和Leave操作导致, 也可能由Move操作导致
	*/
	sm.adjustConfig(&config)
	DPrintf("####Server%dleave后config.Num%d#config.Shards%+v\n", sm.me, config.Num, config.Shards)
	sm.configs = append(sm.configs, config)
}

//  @Description: 调用时必须持有锁
//  @receiver sm
//  @param args
//
func (sm *ShardMaster) move(args MoveArgs) {
	config := sm.configs[len(sm.configs)-1].Copy()
	config.Num = len(sm.configs)
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

//  @Description: 调用时必须持有锁
//  @receiver sm
//  @param num desired config number
//  @return config
//
func (sm *ShardMaster) query(args QueryArgs) (config Config) {
	/*
		If the number is -1 or bigger than the biggest known configuration number, the shardmaster should reply with
		the latest configuration.
	*/
	if args.Num < 0 || args.Num >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	}
	return sm.configs[args.Num].Copy()
}

func (sm *ShardMaster) executeCommand() {
	/*
		applyCh是tester或service期望Raft发送ApplyMsg消息的通道
		当每个Raftpeer意识到日志条目被提交时，peer应该通过传递给Make()的applyCh向同一服务器上的service（或tester）发送ApplyMsg
		applyCh中的附加日志已经处于committed状态, 需要在server中执行该附加日志中的指令
	*/
	for applyMsg := range sm.applyCh {
		// 如果是快照
		if !applyMsg.CommandValid {
			continue
		}

		command := applyMsg.Command
		if command == nil {
			continue
		}
		op := command.(Op)

		sm.mu.Lock()
		switch op.OpType {
		case "Join":
			// 如果已经执行过, 那么不重复执行
			if sm.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			sm.join(op.Args.(JoinArgs))
			sm.maxSequenceNum[op.ClientId] = op.SequenceNum
		case "Leave":
			// 如果已经执行过, 那么不重复执行
			if sm.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			sm.leave(op.Args.(LeaveArgs))
			sm.maxSequenceNum[op.ClientId] = op.SequenceNum
		case "Move":
			// 如果已经执行过, 那么不重复执行 这里需不需要这样存疑
			if sm.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			sm.move(op.Args.(MoveArgs))
			sm.maxSequenceNum[op.ClientId] = op.SequenceNum
		case "Query":
			// 如果已经执行过, 那么不重复执行 这里需不需要这样存疑
			if sm.maxSequenceNum[op.ClientId] >= op.SequenceNum {
				break
			}
			sm.maxSequenceNum[op.ClientId] = op.SequenceNum
		default:
			// 不需要有default
		}
		sm.mu.Unlock()
		/*
			对于相同ClientId和SequenceNum的Append/Put命令, 可能会有多个通道吗
			假设命令已经被执行, 但是由于网络延迟, 导致响应RPC没有及时到达, Client会重发请求, 这时已经不再需要重复执行命令,
			但是仍旧需要向通道发送Success消息, 否则会导致Get/PutAppend阻塞
		*/
		val, ok := sm.indexToCh.Load(applyMsg.CommandIndex)
		if !ok {
			/*
					如果index对应的通道不存在, 则新建通道
					这里选择缓冲通道/无缓冲通道对读取没有差别, 但是对写有差别. 选择缓冲通道的好处是, 写完以后不用等读取就可以继续执行
				    同一条命令可能会在日志里存储两次, 于是会有两个索引, 这两个索引对应的通道都需要发送消息
			*/
			sm.indexToCh.Store(applyMsg.CommandIndex, make(chan Op, 1))
			val, _ = sm.indexToCh.Load(applyMsg.CommandIndex)
		}
		ch := val.(chan Op)
		ch <- op
		close(ch)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	// 注意这里调用了labgob.Register来注册Op结构体, 任何出现在Command中的结构体都需要调用labgob.Register注册
	labgob.Register(Op{})
	// 在这里新建了一个ApplyMsg通道, 然后把它作为参数传给了raft
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.configs[0].Num = 0
	sm.maxSequenceNum = make(map[int]int)
	go sm.executeCommand()

	return sm
}
