package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex // Lock to protect shared access to this Clerk's state
	leaderId int        // leaderId that Clerk thinks
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}

	for {
		ck.mu.Lock()
		idx := ck.leaderId
		ck.mu.Unlock()
		ok := ck.servers[idx].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == "OK" {
			return reply.Value
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// args和 reply的类型（包括它们是否是指针）必须与RPC处理函数参数的声明类型匹配。并且回复必须作为指针传递
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}

	for {
		ck.mu.Lock()
		idx := ck.leaderId
		ck.mu.Unlock()
		ok := ck.servers[idx].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == "OK" {
			break
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
