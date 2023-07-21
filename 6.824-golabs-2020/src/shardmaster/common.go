package shardmaster

import "../labgob"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Copy() Config {
	config := Config{Num: c.Num, Shards: c.Shards, Groups: make(map[int][]string)}
	for k := range c.Groups {
		config.Groups[k] = append([]string{}, c.Groups[k]...)
	}
	return config
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type CommonArgs struct {
	ClientId    int // client invoking request
	SequenceNum int // to eliminate duplicates
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	CommonArgs
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	CommonArgs
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	CommonArgs
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	CommonArgs
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

/*
init()函数会在包被初始化后自动执行，并且在main()函数之前执行，但是需要注意的是init()以及main()函数都是无法被显式调用的.
init()不是最先执行的, 在它之前会进行全局变量的初始化.
*/
func init() {
	/*
		call labgob.Register on structures you want Go's RPC library to marshall/unmarshall.
		注意这里调用了labgob.Register来注册Op结构体, 任何出现在Command中的结构体都需要调用labgob.Register注册
		如果这里不调用labgob.Register注册结构体, 那么Raft.persist()会报错编码日志失败, 如下:
		[{Command:<nil> Term:1} {Command:{Args:{Num:-1 CommonArgs:{ClientId:2718134643594539720 SequenceNum:1}}
		OpType:Query ClientId:2718134643594539720 SequenceNum:1} Term:1}]
	*/
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
}
