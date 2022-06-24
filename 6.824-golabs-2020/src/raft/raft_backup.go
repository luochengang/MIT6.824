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

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
/*
在实验 3 中，您将希望在 applyCh 上发送其他类型的消息（例如，快照）； 此时，您可以将字段添加
到 ApplyMsg，但将 CommandValid 设置为 false 以用于这些其他用途。
*/
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

type ServerState int

const (
	Leader ServerState = iota
	Follower
	Candidate
)

type Timer struct {
	startTime time.Time
	timeOut   time.Duration
	r         *rand.Rand
}

// millisecond
const (
	FixedTimeout       = 200
	RandomTimeout      = 200
	HeartbeatPeriod    = 100
	CheckTimeoutPeriod = 20
)

func (t Timer) reset() {
	t.timeOut = FixedTimeout*time.Millisecond + time.Duration(t.r.Int63n(RandomTimeout))*time.Millisecond
	t.startTime = time.Now()
}

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

	// Persistent state on all servers
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
	/*
		log entries; each entry contains command for state machine, and term when entry was
		received by leader (first index is 1)
	*/
	log   []Entry
	state ServerState // At any given time each server is in one of three states: leader, follower, or candidate

	// Volatile state on all servers
	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	timer       Timer // election timer

	// Volatile state on leaders(Reinitialized after election)
	/*
		for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	*/
	nextIndex []int
	/*
		for each server, index of highest log entry known to be replicated on server
		(initialized to 0, increases monotonically)
	*/
	matchIndex []int
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
	isleader = rf.state == Leader

	return term, isleader
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

func (t Timer) expired() bool {
	return time.Now().Sub(t.startTime) > t.timeOut
}

// first index is 1
func (rf *Raft) getLogTerm(index int) int {
	if index-1 < 0 {
		return -1
	}
	return rf.log[index-1].Term
}

func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.timer.reset()
	// rf.persist() 未完成
}

func (rf *Raft) callRequestVote(server int, term int) bool {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.getLogTerm(len(rf.log)),
	}
	rf.mu.Unlock()
	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok {
		return false
	}
	rf.mu.Lock()
	// 如果回复包含更高的任期，则转换为follower
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
	}
	rf.mu.Unlock()
	return reply.VoteGranted
}

func (rf *Raft) periodicHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			//rf.callAppendEntries() 未完成
		} else {
			rf.mu.Unlock()
			return
		}
		time.Sleep(HeartbeatPeriod * time.Millisecond)
	}
}

// 调用者在调用此函数时应持有互斥锁rf.mu
func (rf *Raft) initializeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) kickOffElection() {
	rf.mu.Lock()
	rf.timer.reset()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// rf.persist() 未完成
	term := rf.currentTerm
	//fmt.Printf("[%d] kickOffElection: currentTerm = %d, votedFor = %d, log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	voteCnt := 1
	finished := false
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if finished {
			return
		}
		// 每个peer都开一个goroutine
		go func(server int) {
			// 这里必须每个peer都发送一个投票请求，哪怕已经有了过半数的票
			voteResult := rf.callRequestVote(server, term)
			//fmt.Printf("[%d] kickOffElection: currentTerm = %d, voteResult = %t\n", rf.me, rf.currentTerm, voteResult)
			if !voteResult {
				return
			}
			rf.mu.Lock()
			voteCnt++
			if voteCnt > len(rf.peers)/2 {
				finished = true
			}

			if finished {
				// 仔细检查它是否仍在选举开始时的任期内
				if rf.currentTerm == term {
					// 这里必须初始化Leader
					rf.initializeLeader()
					rf.mu.Unlock()

					// 这里必须向每个peer都发送心跳
					go rf.periodicHeartbeat()
				} else {
					rf.mu.Unlock()
				}
			}
		}(peer)
	}

	//fmt.Printf("[%d] kickOffElection: currentTerm = %d voteCnt = %d\n", rf.me, rf.currentTerm, voteCnt)
}

func (rf *Raft) periodicTimeout() {
	for !rf.killed() {
		// 要读取raft的状态，所以要加锁
		rf.mu.Lock()
		if rf.state != Leader && rf.timer.expired() {
			go rf.kickOffElection()
		}
		rf.mu.Unlock()
		time.Sleep(CheckTimeoutPeriod * time.Millisecond)
	}
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
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.getLogTerm(len(rf.log)) ||
			(args.LastLogTerm == rf.getLogTerm(len(rf.log)) && args.LastLogIndex >= len(rf.log))) {
		rf.state = Follower
		rf.timer.reset()
		rf.votedFor = args.CandidateId
		// rf.persist() 未完成
		reply.VoteGranted = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

// Make
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
/**
 * @Description: 创建一个 Raft 服务器。
Make() 必须快速返回，因此它应该为任何长时间运行
的工作启动 goroutine。为每个新提交的日志条目发送一个 ApplyMsg 到 Make() 的 applyCh 通道参数
 * @param peers 所有 Raft 服务器（包括这个）的端口，所有服务器的 peers[] 数组都具有相同的顺序。
 * @param me peers[me]代表此服务器的端口
 * @param persister 此raft服务器保存的持久状态
 * @param applyCh Raft 发送 ApplyMsg 消息的通道
 * @return *Raft
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.timer = Timer{startTime: time.Now(), r: rand.New(rand.NewSource(int64(me + 1)))}
	rf.timer = Timer{startTime: time.Now(), r: rand.New(rand.NewSource(time.Now().UnixNano()))}
	rf.timer.reset()

	// initialize from state persisted before a crash
	// 未完成
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("[%d] readPersist: currentTerm = %d, votedFor = %d, log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	go rf.periodicTimeout()

	// go rf.applyCommittedEntries()

	return rf
}
