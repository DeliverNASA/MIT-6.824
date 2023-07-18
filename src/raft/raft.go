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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	LEADER    = "leader"
	CANDIDATE = "candidate"
	FOLLOWER  = "follower"
	// HEARTBEAT_INTERNEL = 300
	HEARTBEAT_INTERNEL = 100
)

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

type LogEntry struct {
	Term    int
	Command interface{}
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
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	commitIndex int        // index of highest log entry known to be committed
	lastApplied int        // index of highest log entry applied to state machine
	nextIndex   []int      // for each server, index of the next log entry to send to that server
	matchIndex  []int      // for each server, index of highest log entry known to be replicated on server

	// additional variable
	state           string        // leader, candidate or follower
	HeartBeatChan   chan int      // channel to handle heartbeat
	RequestVoteChan chan int      // channel to handle request vote
	isLeaderChan    chan bool     // channel to control time out election
	electionCount   int           // record candidate election round
	applyCh         chan ApplyMsg // send applyMsg to the server

	// constant
	serversNum int // number of peers
	majorNum   int // number of major
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = (rf.state == LEADER)

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
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
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
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// AppendEntries RPC handler
//

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A: only need heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true

	if args.Term < rf.currentTerm {
		DPrintf("[%d] reject [%d] as a leader, because term %d < %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		// block here !!! because current state is LEADER, HeartBeatChan will not be consumed (resolved)
		rf.currentTerm = args.Term

		// 1. old leader receive a heartbeat, change to follower
		// 2. candidiate receive a heartbeat, change to follower and reset its timer to avoid another election
		// 3. follower receive a heartbeat, reset its timer
		if rf.state == LEADER {
			rf.state = FOLLOWER
			rf.votedFor = -1
			DPrintf("[%d] state transform: leader -> follower, term update: %v", rf.me, args.Term)
		} else if rf.state == CANDIDATE {
			rf.HeartBeatChan <- args.LeaderId
			rf.state = FOLLOWER
			rf.votedFor = -1
			DPrintf("[%d] state transform: candidate -> follower, term update: %v", rf.me, args.Term)
		} else {
			rf.HeartBeatChan <- args.LeaderId
		}

		// append log entries
		// 这里应该对比的是leader和follower的相同PrevLogIndex和对应的PrevLogTerm
		lastLogindex := len(rf.logs) - 1
		var compareTerm int
		var compareIndex int
		if lastLogindex < args.PrevLogIndex {
			compareIndex = lastLogindex
		} else {
			compareIndex = args.PrevLogIndex
		}

		// if compareIndex < 0 {
		// 	compareTerm = -1
		// } else {
		// 	compareTerm = rf.logs[compareIndex].Term
		// }
		compareTerm = rf.logs[compareIndex].Term

		DPrintf("[%d] receive logs..., (lastLogindex: %v, PrevLogIndex: %v, compareIndex: %v), (lastTerm: %v, PrevLogTerm: %v)", rf.me, lastLogindex, args.PrevLogIndex, compareIndex, compareTerm, args.PrevLogTerm)

		// 1. compareIndex < PrevLogIndex，说明回退不够，要继续回退
		// 2. compareIndex = PrevLogIndex，继续判断compareTerm是否相等，相等就直接添加，否则继续回退
		if compareIndex < args.PrevLogIndex {
			reply.Success = false
		} else {
			if compareTerm != args.PrevLogTerm {
				reply.Success = false
			} else {
				if len(args.Entries) > 0 {
					new_logs := rf.logs[:compareIndex+1]
					rf.logs = append(new_logs, args.Entries...)
					// DPrintf("[%d] append new log: %v, all logs: %v", rf.me, args.Entries, rf.logs)
					DPrintf("[%d] append new log: %v", rf.me, args.Entries)
				}
			}
		}

		// commitIndex update
		DPrintf("[%d] receive leader commitId: %d, self.commidId: %d", rf.me, args.LeaderCommit, rf.commitIndex)
		if reply.Success && args.LeaderCommit > rf.commitIndex {
			lastID := len(rf.logs) - 1
			var commitIndex int
			if args.LeaderCommit < lastID {
				commitIndex = args.LeaderCommit
			} else {
				commitIndex = lastID
			}
			// apply message
			for i := rf.commitIndex + 1; i <= commitIndex; i++ {
				commitLog := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
				rf.applyCh <- commitLog
				DPrintf("[%d] commit log, id: %d, content: %v", rf.me, i, commitLog)
			}
			rf.commitIndex = commitIndex
			DPrintf("[%d] update commitIndex: %d", rf.me, rf.commitIndex)
		}
	}

	reply.Term = rf.currentTerm // attention!!!
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) CallAppendEntries(server int) bool {
	// DPrintf("[%d] sending AppendEntries to %d", rf.me, server)

	if rf.state != LEADER {
		// DPrintf("[%d] no longer a leader", rf.me)
		return false
	}

	// DPrintf("[%d] sending Heartbeat to [%d] in term %d", rf.me, server, rf.currentTerm)

	var prevlogindex int
	var prevlogterm int
	var entries []LogEntry

	length := len(rf.logs)
	nextID := rf.nextIndex[server]

	// 由于有一条空log，所以length > 0
	prevlogterm = rf.logs[nextID-1].Term
	prevlogindex = nextID - 1
	if nextID >= length {
		entries = []LogEntry{}
	} else {
		entries = rf.logs[nextID:]
	}
	// DPrintf("[%d] all logs: %v", rf.me, rf.logs)
	// DPrintf("[%d] prepare for [%d]..., prevlogterm: %v, prevlogindex: %v, log entries: %v", rf.me, server, prevlogterm, prevlogindex, entries)
	DPrintf("[%d] prepare for [%d]..., prevlogterm: %v, prevlogindex: %v, logs: %d -> %d", rf.me, server, prevlogterm, prevlogindex, nextID, length-1)

	// TODO: 2B, 2C
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevlogindex,
		PrevLogTerm:  prevlogterm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, &args, &reply)
	// DPrintf("[%d] finish sending Heartbeat to [%d] in term %d", rf.me, server, rf.currentTerm)
	// DPrintf("[%d] finish sending AppendEntries to [%d] in term %d", rf.me, server, rf.currentTerm)
	if !ok {
		// DPrintf("[%d] fail to call AppendEntries RPC on [%d]", rf.me, server)
		return false
	}
	// DPrintf("[%d] receive follower [%d] reply, reply.term = %d", rf.me, server, reply.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER && rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		DPrintf("[%d] state transform: leader -> follower, term update: %v", rf.me, reply.Term)
	} else {
		// success: count
		// fail: roll back
		if reply.Success {
			// 注意这个地方两个数组的不同，nextIndex更新的是当前的logs数，matchIndex更新的是已经成功同步的logs数
			// nextIndex可以主导下一次同步
			// rf.nextIndex[server] = len(rf.logs)
			rf.nextIndex[server] = length
			rf.matchIndex[server] = length - 1
		} else {
			// rf.nextIndex[server]--
			tmpIndex := rf.nextIndex[server]
			if tmpIndex >= length {
				tmpIndex = length - 1
			}
			tmpTerm := rf.logs[tmpIndex].Term
			var i int
			// 快速同步，从上一个term的最后一个log开始发送
			for i = tmpIndex; i > 1 && rf.logs[i].Term == tmpTerm; i-- {
			}
			rf.nextIndex[server] = i
			DPrintf("[%d] nextIndex[%d] = %d", rf.me, server, i)
		}
	}
	return reply.Success
}

func (rf *Raft) SpreadHeartBeat() {
	DPrintf("[%d] spreads heartbeat at term %d", rf.me, rf.currentTerm)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			ok := rf.CallAppendEntries(server)
			if ok {
				DPrintf("[%d] receives reply from [%d], heartbeat", rf.me, server)
			}
		}(server)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.RequestVoteChan <- args.CandidateID

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get candidate's info
	candidateID := args.CandidateID
	candidateTerm := args.Term
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	// If votedFor is null or candidateId, and candidate’s log is
	// at least as up-to-date as receiver’s log, grant vote

	// 1. verify Term >= currentTerm
	// 2. verify no voting for other candidate in the same term
	// 3. satisfy at least one of the following two conditions
	//    (a) candidateLastLogTerm > currentLastLogTerm
	//    (b) candidateLastLogTerm = currentLastLogTerm but higher or equal LogIndex

	length := len(rf.logs)

	if rf.currentTerm > candidateTerm {
		reply.VoteGranted = false
		DPrintf("[%d] reject candidate [%d]: term lower", rf.me, candidateID)
		// } else if rf.votedFor == -1 || rf.votedFor == candidateID {
	} else {
		// if candidate has a higher term, server regets a vote
		if rf.currentTerm < candidateTerm {
			rf.votedFor = -1
			rf.currentTerm = candidateTerm
		}
		if rf.votedFor == -1 {
			// 1. server with no logs will always vote for any candidate
			// 2. server votes for candidate that satisfies at least one of the two conditions
			// 3. otherwise reject the candidate
			DPrintf("[%d] candidate vs local: LastLogTerm (%d, %d), LastLogIndex: (%d, %d)", rf.me, candidateLastLogTerm, rf.logs[length-1].Term, candidateLastLogIndex, length-1)
			if rf.logs[length-1].Term < candidateLastLogTerm || (rf.logs[length-1].Term == candidateLastLogTerm && (length-1) <= candidateLastLogIndex) {
				reply.VoteGranted = true
				rf.votedFor = candidateID
				DPrintf("[%d] vote for [%d] in term %d", rf.me, candidateID, rf.currentTerm)
			} else {
				reply.VoteGranted = false
				DPrintf("[%d] reject candidate [%d] in term %d: logs lower", rf.me, candidateID, rf.currentTerm)
			}
		} else {
			reply.VoteGranted = false
			DPrintf("[%d] reject candidate [%d] in term %d: have voted [%d]", rf.me, candidateID, rf.currentTerm, rf.votedFor)
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
func (rf *Raft) CallRequestVote(server int) bool {
	rf.mu.Lock()
	length := len(rf.logs)
	// DPrintf("[%d] sending request vote to [%d]", rf.me, server)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: length - 1,
		LastLogTerm:  rf.logs[length-1].Term,
	}
	rf.mu.Unlock()

	var reply RequestVoteReply

	// if server is disconnected, sendRequestVote() will be blocked
	ok := rf.sendRequestVote(server, &args, &reply)
	// DPrintf("[%d] finish sending request vote to [%d]", rf.me, server)
	if !ok {
		// DPrintf("[%d] fail to call request vote RPC on [%d]", rf.me, server)
		return false
	}
	// if RPC success, return VoteGranted (maybe true or false)
	// if VoteGranted == false, maybe need to update candidate's term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		DPrintf("[%d] state transform: candidate -> follower, term update: %v", rf.me, reply.Term)
	}

	return reply.VoteGranted
}

func (rf *Raft) AttemptElection() bool {

	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.electionCount++
	curRound := rf.electionCount
	DPrintf("[%d] attempting an election %d at term [%d]", rf.me, rf.electionCount, rf.currentTerm)
	rf.mu.Unlock()

	count := 1 // get default vote from self
	finished := 1
	var mu sync.Mutex
	// use condition signal
	cond := sync.NewCond(&mu)

	for server, _ := range rf.peers {
		// needn't send RPC to self, send RPCs to other servers
		if server == rf.me {
			continue
		}
		go func(server int) {
			// when rf.state changed during election, stop the remaining process
			// attention: judge here ???
			// if rf.state != CANDIDATE {
			// 	// DPrintf("[%d] no more be a candidate, stop requesting votes.", rf.me)
			// 	return
			// }
			// RPC call may fail, but we don't care about it beacause we only need to know whether it votes
			VoteGranted := rf.CallRequestVote(server)
			mu.Lock()
			defer mu.Unlock()
			if VoteGranted {
				count++
			}
			finished++
			// DPrintf("[%d] receives info from [%d], vote = %v, count = %d, finished = %d", rf.me, server, VoteGranted, count, finished)
			cond.Broadcast()
		}(server)
	}

	// collect votes
	mu.Lock()
	for count < rf.majorNum && finished != rf.serversNum {
		cond.Wait()
	}
	mu.Unlock()

	// While getting enough votes, become a new leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if count >= rf.majorNum {
		rf.state = LEADER
		rf.isLeaderChan <- true
		for i := 0; i < rf.serversNum; i++ {
			rf.nextIndex[i] = len(rf.logs)
			// 如果是自己，匹配的id是最后一条log的id
			// 如果是别人，匹配的id是0
			if i == rf.me {
				rf.matchIndex[i] = len(rf.logs) - 1
			} else {
				rf.matchIndex[i] = 0
			}
		}
		DPrintf("[%d] state transform: candidate -> leader", rf.me)
	}

	DPrintf("[%d] finish election %d, safely exits", rf.me, curRound)
	return true
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
	defer rf.mu.Unlock()

	term, isLeader = rf.GetState()
	if isLeader {
		index = len(rf.logs)
		term = rf.currentTerm
		logentry := LogEntry{Term: term, Command: command}
		rf.matchIndex[rf.me] = len(rf.logs) // 这里要更新自己的matchIndex，否则在agreement的时候会失败
		rf.logs = append(rf.logs, logentry)
		DPrintf("[%d] append new log, term: %v, command: %v", rf.me, logentry.Term, logentry.Command)
	}

	return index, term, isLeader
}

func (rf *Raft) CommmitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	max_logIndex := -1
	commit_logIndex := -1

	DPrintf("[%d] matchIndex: %v", rf.me, rf.matchIndex)
	for i := 0; i < rf.serversNum; i++ {
		if rf.matchIndex[i] > max_logIndex {
			max_logIndex = rf.matchIndex[i]
		}
	}

	for j := max_logIndex; j > rf.commitIndex; j-- {
		count := 0
		for i := 0; i < rf.serversNum; i++ {
			if rf.matchIndex[i] >= j {
				count++
			}
		}
		if count >= rf.majorNum {
			commit_logIndex = j
			break
		}
	}

	if commit_logIndex > rf.commitIndex {
		DPrintf("[%d] commits logs: %d -> %d", rf.me, rf.commitIndex+1, commit_logIndex)
		for i := rf.commitIndex + 1; i <= commit_logIndex; i++ {
			commitLog := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			rf.applyCh <- commitLog
			DPrintf("[%d] commit log, id: %d, content: %v", rf.me, i, commitLog)
		}
		rf.commitIndex = commit_logIndex
	}
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

// generate a time out between 600 and 800 ms
func GetRandTimeOut() time.Duration {
	return time.Duration(rand.Intn(201)+600) * time.Millisecond
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

	rf.serversNum = len(rf.peers)
	rf.majorNum = int(math.Ceil(float64(rf.serversNum) / 2))

	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{Term: -1, Command: -1}) // fill the blank
	rf.commitIndex = 0
	rf.nextIndex = make([]int, rf.serversNum)
	rf.matchIndex = make([]int, rf.serversNum)

	rf.HeartBeatChan = make(chan int)
	rf.RequestVoteChan = make(chan int)
	rf.isLeaderChan = make(chan bool)
	rf.electionCount = 0
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	reset := make(chan bool)

	var timer *time.Timer
	isTimerStop := false

	// background goroutine 1: server operation
	go func() {
		for {
			_, isleader := rf.GetState()
			// DPrintf("[%d] is leader? %v", rf.me, isleader)
			if !isleader {
				timer = time.NewTimer(GetRandTimeOut())
				isTimerStop = false
				select {
				// 1. receive heartbeat signal -> reset the timer
				case leaderID := <-rf.HeartBeatChan:
					DPrintf("[%d] receive a heartbeat from [%d] in term %d", rf.me, leaderID, rf.currentTerm)
					reset <- true
				// 2. receive request vote signal -> reset the timer
				case candidateID := <-rf.RequestVoteChan:
					DPrintf("[%d] receive a request vote from [%d] in term %d", rf.me, candidateID, rf.currentTerm)
					reset <- true
				// 3. reach a time out -> kick off a election
				case <-timer.C:
					DPrintf("[%d] time out", rf.me)
					reset <- true
					go rf.AttemptElection()
				// 4. receive a isleader signal -> reset the timer and later remove it
				case isleader = <-rf.isLeaderChan:
					DPrintf("[%d] become a leader", rf.me)
					reset <- true
				}
			} else {
				// leader doesn't need timer
				if !isTimerStop {
					timer.Stop()
					isTimerStop = true
					// DPrintf("[%d] stop timer", rf.me)
				}

				// commits logs
				DPrintf("[%d] check whether to commit", rf.me)
				rf.CommmitLog()

				// spreads HeartBeat
				DPrintf("[%d] now spread Heartbeat", rf.me)
				rf.SpreadHeartBeat()

				time.Sleep(time.Duration(HEARTBEAT_INTERNEL) * time.Millisecond)
			}
		}
	}()

	// background goroutine 2: reset timer
	go func() {
		for {
			select {
			case <-reset:
				// DPrintf("[%d] reset timer", rf.me)
				timer.Reset(GetRandTimeOut())
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
