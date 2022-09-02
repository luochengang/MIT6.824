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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader (first index is 1)
}

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

// ms
const (
	CheckElectTimeout = 10
	ElectTimeoutMin   = 210
	ElectTimeoutMax   = 710
	HeartbeatTimeout  = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers (Updated before responding to RPCs)
	currentTerm int // latest term server has seen. initialized to 0 on first boot
	votedFor    int // candidateId that received vote in current term (or null if none)
	/*
	   log entries; each entry contains command for state machine, and term when entry was received
	   by leader (first index is 1)
	*/
	log []Log

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed, initialized to 0
	lastApplied int // index of highest log entry applied to state machine, initialized to 0

	// Volatile state on leaders(Reinitialized after election)
	/*
	   for each server, index of the next log entry to send to that server (initialized
	   to leader last log index + 1)
	*/
	nextIndex  []int
	matchIndex []int // for each server, index of highest log entry known to be replicated on server, initialized to 0

	role         Role       // current role
	electTimeout time.Time  // 选举超时时间
	cond         *sync.Cond // Leader收到新Log时将激活该条件变量
	applyCh      chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
/**
 * @Description: 在实现更改持久状态的点插入对 persist() 的调用, 调用时必须持有锁
 */
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		fmt.Printf("####Server%d编码任期%d失败\n", rf.me, rf.currentTerm)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		fmt.Printf("####Server%d编码投票%d失败\n", rf.me, rf.votedFor)
	}
	err = e.Encode(rf.log)
	if err != nil {
		fmt.Printf("####Server%d编码日志%+v失败\n", rf.me, rf.log)
	}
	data := w.Bytes()
	//fmt.Printf("####Server%d持久化任期%d投票%d日志%+v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	//fmt.Printf("####Server%d持久化任期%d投票%d日志长度%d\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("####decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		//fmt.Printf("####Server%d读取任期%d投票%d日志%+v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
		//fmt.Printf("####Server%d读取任期%d投票%d日志长度%d\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if rf.currentTerm < args.Term {
		if rf.role == Leader {
			fmt.Printf("####此时Leader%d任期%d变更为Follower任期%d\n", rf.me, rf.currentTerm, args.Term)
		}
		rf.convertToFollower(args.Term, -1)
	}

	/*
			根据日志新的程度决定是否投票
		    这里需要考虑RPC响应失败后, 收到相同RPC的情况, 不过这里是幂等的
			这里的rf.votedFor == args.CandidateId就是考虑RPC响应可能丢失的情况
	*/
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > getLastLogTerm(rf.log) || (args.LastLogTerm == getLastLogTerm(rf.log) &&
			args.LastLogIndex >= len(rf.log))) {
		/*
			fmt.Printf("####Server%d最后日志任期%d给Candidate%d最后日志任期%d投票\n", rf.me,
				getLastLogTerm(rf.log), args.CandidateId, args.LastLogTerm)
			fmt.Printf("####此时Server%d的日志为%+v\n", rf.me, rf.log)
		*/
		rf.convertToFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
	}
}

/*
	如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们存储了相同的指令
	如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们之前的所有日志条目也全部相同
	先发出的AppendEntries RPC不一定先到达
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Leader任期 < Follower任期
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	rf.convertToFollower(args.Term, args.LeaderId)
	//fmt.Printf("####Server%d任期%d的日志为%+v\n", rf.me, rf.currentTerm, rf.log)

	preLogLen := len(rf.log)
	if args.PrevLogIndex == 0 {
		reply.Success = true
		fmt.Printf("####Server%d任期%d的日志变更前为%+v\n", rf.me, rf.currentTerm, rf.log)
		// 复制log到Follower
		// 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		rf.log = nil
		rf.log = append(rf.log, args.Entries...)
		fmt.Printf("####Server%d任期%d的日志变更后为%+v\n", rf.me, rf.currentTerm, rf.log)
		fmt.Printf("####此时Leader%d任期%d\n", args.LeaderId, args.Term)
	} else if args.PrevLogIndex > len(rf.log) {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return
	} else if rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
		reply.Success = true
		if len(args.Entries) > 0 {
			fmt.Printf("####Server%d任期%d的日志变更前为%+v\n", rf.me, rf.currentTerm, rf.log)
		}
		// 这里需要考虑RPC响应失败后, 收到相同RPC的情况, 不过这里是幂等的
		/*
			复制log到Follower
			Figure7: 对于Follower c和d，如果收到了leader发送的AppendEntries RPC，会把leader没有的那部分日志删除;
			如果Leader向Follower发送了第一个AppendEntries RPC，然后Leader在日志中新增了一个条目，然后Leader向Follower发送了
			第二个AppendEntries RPC，但是第二个RPC比第一个先到。那么Follower收到第一个RPC时，需要把已经添加到日志中的条目给删除
		*/
		// 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		rf.log = rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
		if len(args.Entries) > 0 {
			fmt.Printf("####Server%d任期%d的日志变更后为%+v\n", rf.me, rf.currentTerm, rf.log)
			fmt.Printf("####此时Leader%d任期%d\n", args.LeaderId, args.Term)
		}
	} else {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		return
	}

	// 服务器只在日志和Leader匹配的情况下更新commitIndex和持久化
	if len(args.Entries) > 0 || preLogLen != len(rf.log) {
		/*
			只在日志匹配并且发生变化时持久化, 心跳时不需要持久化
			这里可能args.Entries长度为0也需要持久化, 比如Figure7的c和d
		*/
		//fmt.Printf("####Server%d任期%d的日志长度由%d变更为%d\n", rf.me, rf.currentTerm, preLogLen, len(rf.log))
		rf.persist()
	}
	// rf.commitIndex可能变小, 然后导致越界错误
	rf.commitIndex = min(rf.commitIndex, len(rf.log))
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		fmt.Printf("####Server%d任期%d的commitIndex变更为%d\n", rf.me, rf.currentTerm, rf.commitIndex)
		// 这里已经用rf.mu.Lock()加锁了, 不需要再用rf.cond.L.Lock()加锁, 否则会导致死锁
		rf.cond.Signal()
	}
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
//
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
	// 在这里调用了func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return index, term, false
	}

	rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
	index = len(rf.log)
	term = rf.currentTerm
	rf.matchIndex[rf.me] = len(rf.log)
	rf.nextIndex[rf.me] = len(rf.log) + 1
	rf.persist()
	rf.mu.Unlock()

	rf.appendLog()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 调用时必须持有锁
func getLastLogTerm(log []Log) int {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Term
}

/**
 * @Description: Leader给其他服务器发送心跳消息
 */
func (rf *Raft) appendLog() {
	rf.mu.Lock()
	fmt.Printf("####Leader%d任期%d的日志为%+v\n", rf.me, rf.currentTerm, rf.log)
	rf.mu.Unlock()
	for i := range rf.peers {
		// 不用给自己发心跳
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			// !!!这里再次获取锁以后, 可能已经不是leader了
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			var prevLogTerm int
			if prevLogIndex >= 1 {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
			entries := make([]Log, len(rf.log)-rf.nextIndex[server]+1)
			for j := range entries {
				entries[j] = rf.log[rf.nextIndex[server]-1+j]
			}

			args := &AppendEntriesArgs{Term: rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex}
			nextIdx := rf.nextIndex[server]
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			//fmt.Printf("####Leader%d任期%d发出的日志长度为%d\n", rf.me, rf.currentTerm, len(args.Entries))
			//fmt.Printf("####Leader%d任期%d#%d#%d\n", rf.me, rf.currentTerm, len(rf.log), rf.nextIndex[server])
			// 这里失败了不需要重发
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 这里再次获取锁以后, 可能已经不是leader了
			if rf.role != Leader {
				return
			}
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				fmt.Printf("####此时Leader%d任期%d变更为Follower任期%d\n", rf.me, rf.currentTerm, reply.Term)
				rf.convertToFollower(reply.Term, -1)
				return
			}

			// 这里的rf.nextIndex[server]可能和发送RPC之前的rf.nextIndex[server]不一样了
			if rf.nextIndex[server] != nextIdx {
				return
			}
			// 这里不需要统计是否半数以上服务器返回了成功
			if reply.Success {
				rf.nextIndex[server] += len(entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
			}
		}(i)
	}
	rf.updateLeaderCommitIndex()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.role = Leader

	rf.nextIndex = nil
	rf.matchIndex = nil
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log)+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.matchIndex[rf.me] = len(rf.log)
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.appendLog()
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
}

/**
 * @Description:
 */
func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}
	for idx, v := range rf.matchIndex {
		fmt.Printf("####Server%d任期%d的matchIndex是%d\n", idx, rf.currentTerm, v)
		//fmt.Printf("####Server%d任期%d的nextIndex是%d\n", idx, rf.currentTerm, rf.nextIndex[idx])
	}

	maxCommitIdx := len(rf.log)
	for maxCommitIdx > 0 {
		replicaCnt := 0
		for _, v := range rf.matchIndex {
			if v >= maxCommitIdx {
				replicaCnt++
			}
		}

		// 只有当前任期的log用统计是否过半来决定是否commit, 之前任期的log被动commit
		if replicaCnt > len(rf.peers)/2 && rf.log[maxCommitIdx-1].Term == rf.currentTerm &&
			maxCommitIdx > rf.commitIndex {
			fmt.Printf("####Leader%d任期%d的commitIndex变更为%d\n", rf.me, rf.currentTerm, maxCommitIdx)
			rf.commitIndex = maxCommitIdx
			rf.cond.Signal()
			return
		}
		maxCommitIdx--
	}
}

/**
 * @Description: 服务器选举超时, 开始选举
 */
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	// 刚开始选举时的任期
	term := rf.currentTerm
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.resetTimeout()
	rf.persist()

	countVote := 1
	done := false
	// 如果只有1台服务器
	if len(rf.peers) == 1 {
		// 服务器还是选举刚开始时的任期, 还是Candidate, 获得了过半的投票, 变成Leader
		fmt.Printf("####Candidate%d任期%d成为Leader\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		go rf.becomeLeader()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		// 不用给自己发
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			// 如果这里已经不是Candidate了, 那么释放锁后直接返回, 因为只有Candidate才需要发送RequestVote RPC
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}

			args := &RequestVoteArgs{Term: rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log),
				LastLogTerm:  getLastLogTerm(rf.log)}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			/*
				这里如果没收到响应, 不需要一直发
			*/
			received := rf.sendRequestVote(server, args, reply)
			if !received {
				return
			}

			rf.mu.Lock()
			// 如果这里已经不是Candidate了, 那么释放锁后直接返回, 因为只有Candidate才需要发送RequestVote RPC
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			// 如果RPC响应的任期号比自己更新, 那么变成Follower
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term, -1)
			}
			if reply.VoteGranted {
				fmt.Printf("####Server%d任期%d投票给Candidate%d任期%d\n", server, reply.Term, rf.me, rf.currentTerm)
				countVote++
			}

			if done || countVote <= len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
			done = true
			if rf.currentTerm == term {
				// 服务器还是选举刚开始时的任期, 还是Candidate, 获得了过半的投票, 变成Leader
				fmt.Printf("####Candidate%d任期%d成为Leader\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				go rf.becomeLeader()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}
}

/**
 * @Description: 服务器周期性检查是否选举超时
 */
func (rf *Raft) checkElectLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader && rf.expired() {
			go rf.startElection()
		}
		rf.mu.Unlock()
		// 每10ms检查一次 这里可以优化吗? 用条件变量?
		time.Sleep(time.Duration(CheckElectTimeout) * time.Millisecond)
	}
}

/**
 * @Description: 调用时必须持有锁
 */
func (rf *Raft) resetTimeout() {
	randTime := ElectTimeoutMin + rand.Intn(ElectTimeoutMax-ElectTimeoutMin)
	now := time.Now()
	rf.electTimeout = now.Add(time.Duration(randTime) * time.Millisecond)
}

/**
 * @Description: 检查选举定时器是否超时, 调用时必须持有锁
 * @return bool
 */
func (rf *Raft) expired() bool {
	return time.Now().After(rf.electTimeout)
}

/**
 * @Description: 调用时必须持有锁
 * @param newTerm 新的任期
 * @param newVotedFor 投票服务器
 */
func (rf *Raft) convertToFollower(newTerm, newVotedFor int) {
	preTerm := rf.currentTerm
	preVotedFor := rf.votedFor

	rf.currentTerm = newTerm
	rf.role = Follower
	rf.votedFor = newVotedFor
	rf.resetTimeout()
	// 只在任期或者投票发生变化时持久化
	if preTerm != newTerm || preVotedFor != newVotedFor {
		rf.persist()
	}
}

/**
 * @Description: 服务器应用已经提交的日志
 */
func (rf *Raft) applyCommittedEntries() {
	for !rf.killed() {
		// 这个方法里加锁的方式可以优化吗
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}

		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.commitIndex <= rf.lastApplied {
				rf.mu.Unlock()
				break
			}
			/*
				fmt.Printf("####Server%d任期%d日志长度%dcommitIndex%dlastApplied%d\n", rf.me, rf.currentTerm,
					len(rf.log), rf.commitIndex, rf.lastApplied)
			*/
			// If commitIndex > lastApplied: increment lastApplied, apply
			// log[lastApplied] to state machine (§5.3)
			applyMsg := ApplyMsg{CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			//fmt.Printf("####Server%d任期%d提交了新的日志%+v\n", rf.me, rf.currentTerm, applyMsg)
			rf.lastApplied++
			rf.mu.Unlock()
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//这里不能直接调用rf.convertToFollower(0, -1), 否则会导致调用rf.persist()而覆盖之前的持久化结果
	rf.currentTerm = 0
	rf.role = Follower
	rf.votedFor = -1
	rf.resetTimeout()
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	// 先读取持久化存储, 再调用后面的方法
	rf.readPersist(persister.ReadRaftState())

	go rf.checkElectLoop()
	// 应用已经提交的日志
	go rf.applyCommittedEntries()

	return rf
}
