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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
	CheckElectTimeout = 30
	ElectTimeoutMin   = 300
	ElectTimeoutMax   = 450
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

	role         Role      // current role
	electTimeout time.Time // 选举超时时间
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

	if rf.currentTerm < args.Term {
		if rf.role != Follower {
			rf.role = Follower
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	/*
		// 根据日志新的程度决定是否投票
		if args.LastLogTerm > getLastLogTerm(rf.log) || (args.LastLogTerm == getLastLogTerm(rf.log) &&
			args.LastLogIndex >= len(rf.log)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	*/

	// 更新自己的当前任期
	rf.currentTerm = max(rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm

	// 刷新选举超时时间
	randTime := ElectTimeoutMin + rand.Intn(ElectTimeoutMax-ElectTimeoutMin)
	now := time.Now()
	rf.electTimeout = now.Add(time.Duration(randTime) * time.Millisecond)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Follower任期 > Leader任期
		reply.Success = false
	} else {
		// Candidate状态下收到了心跳
		if rf.role == Candidate {
			rf.role = Follower
		}

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}

		// 刷新选举超时时间
		randTime := ElectTimeoutMin + rand.Intn(ElectTimeoutMax-ElectTimeoutMin)
		now := time.Now()
		rf.electTimeout = now.Add(time.Duration(randTime) * time.Millisecond)
		reply.Success = true
	}

	reply.Term = rf.currentTerm
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

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.role = Leader
	rf.mu.Unlock()

	for i := range rf.peers {
		go func(server int) {
			// 不用给自己发心跳
			if server == rf.me {
				return
			}

			for rf.role == Leader {
				time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)

				rf.mu.Lock()
				args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				// 这里失败了需要重发吗
				rf.sendAppendEntries(server, args, reply)

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.role = Follower
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	serverNum := len(rf.peers)

	randTime := ElectTimeoutMin + rand.Intn(ElectTimeoutMax-ElectTimeoutMin)
	now := time.Now()
	rf.electTimeout = now.Add(time.Duration(randTime) * time.Millisecond)
	rf.mu.Unlock()

	countVote := 1
	var wg sync.WaitGroup

	wg.Add(serverNum)
	for i := range rf.peers {
		go func(server int) {
			defer wg.Done()
			rf.mu.Lock()

			// 不用给自己发
			if server == rf.me {
				rf.mu.Unlock()
				return
			}

			// 如果已经是Follower, 那么不再发送RequestVote RPC
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
				这里如果没收到响应, 需要一直发吗; 如果一直发, 可能导致这个goroutine一直无法结束, 从而无法统计投票结果
				所以不一直发
			*/
			received := rf.sendRequestVote(server, args, reply)
			if !received {
				return
			}

			// 如果RPC响应的任期号比自己更新, 那么变成Follower
			if reply.Term > rf.currentTerm {
				rf.role = Follower
			}
			if reply.VoteGranted {
				countVote++
			}
		}(i)
	}
	wg.Wait()

	if rf.role == Candidate && countVote >= (serverNum-1)/2+1 {
		rf.becomeLeader()
	}
}

func (rf *Raft) checkElectLoop() {
	for {
		time.Sleep(time.Duration(CheckElectTimeout) * time.Millisecond)
		rf.mu.Lock()
		timeout := rf.electTimeout
		rf.mu.Unlock()
		if rf.role == Follower && time.Now().After(timeout) {
			rf.startElection()
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
	rf.role = Follower
	randTime := ElectTimeoutMin + rand.Intn(ElectTimeoutMax-ElectTimeoutMin)
	now := time.Now()
	rf.electTimeout = now.Add(time.Duration(randTime) * time.Millisecond)

	go rf.checkElectLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
