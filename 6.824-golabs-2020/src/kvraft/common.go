package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int // client invoking request (6.3)
	SequenceNum int // to eliminate duplicates ($6.4)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// Get请求也需要保证只被执行一次
	ClientId    int // client invoking request (6.3)
	SequenceNum int // to eliminate duplicates ($6.4)
}

type GetReply struct {
	Err   Err
	Value string
}
