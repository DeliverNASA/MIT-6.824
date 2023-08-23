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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	LEADER                           = "leader"
	CANDIDATE                        = "candidate"
	FOLLOWER                         = "follower"
	OPTIMIZATION                     = false
	HEARTBEAT_INTERNEL               = 50
	COMMIT_INTERNEL                  = 100
	BASE_ELECTION_TIMEOUT            = 300
	MAX_ADDITIONALE_ELECTION_TIMEOUT = 200
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
	currentTerm int           // latest term server has seen
	votedFor    int           // candidateId that received vote in current term
	logs        []LogEntry    // log entries; each entry contains command for state machine, and term when entry was received by leader
	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	nextIndex   []int         // for each server, index of the next log entry to send to that server
	matchIndex  []int         // for each server, index of highest log entry known to be replicated on server
	applyCh     chan ApplyMsg // send applyMsg to the server

	// additional variable
	state                 string    // leader, candidate or follower
	HeartBeatChan         chan int  // channel to handle heartbeat
	RequestVoteChan       chan int  // channel to handle request vote
	isLeaderChan          chan bool // channel to control time out election
	lastAppliedIndex      []int     // for each server, index of highest log entry known to be committed on server
	availableAppliedIndex int       // leader's commit

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.availableAppliedIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var commitIndex int
	var availableAppliedIndex int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil || d.Decode(&availableAppliedIndex) != nil || d.Decode(&logs) != nil {
		DPrintf("Term %d: [%d] fails to decode.", rf.currentTerm, rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		// rf.commitIndex = commitIndex
		if availableAppliedIndex < commitIndex {
			rf.commitIndex = availableAppliedIndex
		} else {
			rf.commitIndex = commitIndex
		}
		rf.availableAppliedIndex = availableAppliedIndex
		rf.logs = logs
	}
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

	ConflictStartIndex int // index of the first log with conflicting term, used for speedup
	LastApplied        int // index of follower's last committed log
}

//
// AppendEntries RPC handler
//

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A: only need heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.ConflictStartIndex = -1

	if args.Term < rf.currentTerm {
		DPrintf("Term %d: [%d] rejects [%d] as a leader, because term %d < %d", rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		// 1. old leader receive a heartbeat, change to follower
		// 2. candidiate receive a heartbeat, change to follower and reset its timer to avoid another election
		// 3. follower receive a heartbeat, reset its timer
		if rf.state == LEADER {
			rf.state = FOLLOWER
			DPrintf("Term %d: [%d] state transform: leader -> follower, term update: %v", rf.currentTerm, rf.me, args.Term)
		} else if rf.state == CANDIDATE {
			rf.HeartBeatChan <- args.LeaderId
			rf.state = FOLLOWER
			DPrintf("Term %d: [%d] state transform: candidate -> follower, term update: %v", rf.currentTerm, rf.me, args.Term)
		} else {
			rf.HeartBeatChan <- args.LeaderId
		}

		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist()

		// get compareIndex and compareTerm
		lastLogindex := len(rf.logs) - 1
		var compareTerm int
		var compareIndex int
		if lastLogindex < args.PrevLogIndex {
			compareIndex = lastLogindex
		} else {
			compareIndex = args.PrevLogIndex
		}
		compareTerm = rf.logs[compareIndex].Term

		DPrintf("Term %d: [%d] receives logs..., (lastLogindex: %v, PrevLogIndex: %v, compareIndex: %v), (compareTerm: %v, PrevLogTerm: %v)", rf.currentTerm, rf.me, lastLogindex, args.PrevLogIndex, compareIndex, compareTerm, args.PrevLogTerm)

		// compare leader's logs with own
		if compareIndex != args.PrevLogIndex || compareTerm != args.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			if len(args.Entries) > 0 {
				new_logs := rf.logs[:compareIndex+1]
				rf.logs = append(new_logs, args.Entries...)
				rf.persist()
				// DPrintf("Term %d: [%d] append new log: %v", rf.currentTerm, rf.me, args.Entries)
				DPrintf("Term %d: [%d] appends new log: %v, all logs: %v", rf.currentTerm, rf.me, args.Entries, rf.logs)
			}
		}

		// reply.success = false: roll back to the possibly conflict index
		// reply.success = true: update commitIndex if possible
		if !reply.Success {
			var startIndex int
			for startIndex = compareIndex; startIndex > 1 && rf.logs[startIndex].Term == compareTerm; startIndex-- {
			}
			if startIndex == 0 {
				reply.ConflictStartIndex = 1
			} else {
				reply.ConflictStartIndex = startIndex
			}
		} else if args.LeaderCommit > rf.commitIndex {
			// DPrintf("Term %d: [%d] receive leader commitId: %d, self.commidId: %d", rf.currentTerm, rf.me, args.LeaderCommit, rf.commitIndex)
			lastID := len(rf.logs) - 1
			old := rf.commitIndex
			if args.LeaderCommit < lastID {
				rf.commitIndex = args.LeaderCommit
				rf.availableAppliedIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastID
				rf.availableAppliedIndex = lastID
			}
			rf.persist()
			DPrintf("Term %d: [%d] updates commitIndex: %d -> %d", rf.currentTerm, rf.me, old, rf.commitIndex)
		}

		DPrintf("Term %d: [%d] sender: %d, all logs: %v", rf.currentTerm, rf.me, args.LeaderId, rf.logs)
		DPrintf("Term %d: [%d] reply success: %v, ConflictStartIndex: %v", rf.currentTerm, rf.me, reply.Success, reply.ConflictStartIndex)

	}

	reply.LastApplied = rf.lastApplied
	reply.Term = rf.currentTerm // attention!!!
}

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

func (rf *Raft) CallAppendEntries(server int) bool {
	// DPrintf("Term %d: [%d] sending AppendEntries to %d", rf.currentTerm, rf.me, server)

	// attention: guarantee term and isLeader are a match
	rf.mu.Lock()
	term, isLeader := rf.GetState()
	rf.mu.Unlock()

	if !isLeader {
		return false
	}

	// DPrintf("Term %d: [%d] sending Heartbeat to [%d] in term %d", rf.currentTerm, rf.me, server, rf.currentTerm)

	var prevlogindex int
	var prevlogterm int
	var entries []LogEntry

	length := len(rf.logs)
	nextID := rf.nextIndex[server]

	prevlogterm = rf.logs[nextID-1].Term
	prevlogindex = nextID - 1
	if nextID >= length {
		entries = []LogEntry{}
	} else {
		entries = rf.logs[nextID:]
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevlogindex,
		PrevLogTerm:  prevlogterm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	var reply AppendEntriesReply
	// ok := rf.sendAppendEntries(server, &args, &reply)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// DPrintf("Term %d: [%d] fails to call AppendEntries RPC on [%d]", rf.currentTerm, rf.me, server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term %d: [%d] state: %v, reply[%d] term %d, success: %v, ConflictStartIndex: %v, length: %v", rf.currentTerm, rf.me, rf.state, server, reply.Term, reply.Success, reply.ConflictStartIndex, length)

	// ignore outdate RPC
	if reply.Term < rf.currentTerm || rf.state != LEADER {
		return false
	}

	// update leader's info
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		DPrintf("Term %d: [%d] state transform: leader -> follower, term update: %v", rf.currentTerm, rf.me, reply.Term)
	} else {
		if reply.Success {
			rf.nextIndex[server] = length
			rf.matchIndex[server] = length - 1
			// avoid previous RPC reply changing lastAppliedIndex
			if reply.LastApplied > rf.lastAppliedIndex[server] {
				rf.lastAppliedIndex[server] = reply.LastApplied
			}
		} else {
			// avoid previous RPC reply changing nextIndex
			if reply.ConflictStartIndex < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.ConflictStartIndex
			}
			// DPrintf("Term %d: [%d] nextIndex[%d] = %d", rf.currentTerm, rf.me, server, reply.ConflictStartIndex)
		}
	}
	return reply.Success
}

func (rf *Raft) SpreadHeartBeat() {
	// DPrintf("Term %d: [%d] spreads heartbeat at term %d", rf.currentTerm, rf.me, rf.currentTerm)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			ok := rf.CallAppendEntries(server)
			if ok {
				// DPrintf("Term %d: [%d] receives reply from [%d], heartbeat", rf.currentTerm, rf.me, server)
			}
		}(server)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// only follower has RequestVoteChan
	if rf.state != LEADER {
		rf.RequestVoteChan <- args.CandidateID
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get candidate's info
	candidateID := args.CandidateID
	candidateTerm := args.Term
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	// 1. verify Term >= currentTerm
	// 2. verify no voting for other candidate in the same term
	// 3. satisfy at least one of the following two conditions
	//    (a) candidateLastLogTerm > currentLastLogTerm
	//    (b) candidateLastLogTerm = currentLastLogTerm && candidateLastLogIndex >= currentLastLogIndex

	length := len(rf.logs)

	if rf.currentTerm > candidateTerm {
		reply.VoteGranted = false
		DPrintf("Term %d: [%d] rejects candidate [%d]: term lower", rf.currentTerm, rf.me, candidateID)
	} else {
		// if candidate has a higher term, server regets a vote
		if rf.currentTerm < candidateTerm {
			rf.votedFor = -1
			rf.currentTerm = candidateTerm
			if rf.state == LEADER {
				rf.state = FOLLOWER
			}
			rf.persist()
		}

		// one server cannot vote for multiple candidates in the same term
		if rf.votedFor == -1 {
			// DPrintf("Term %d: [%d] candidate vs local: LastLogTerm (%d, %d), LastLogIndex: (%d, %d)", rf.currentTerm, rf.me, candidateLastLogTerm, rf.logs[length-1].Term, candidateLastLogIndex, length-1)
			if rf.logs[length-1].Term < candidateLastLogTerm ||
				(rf.logs[length-1].Term == candidateLastLogTerm && (length-1) <= candidateLastLogIndex) {
				// voting for others can reset timer
				// rf.RequestVoteChan <- args.CandidateID
				reply.VoteGranted = true
				rf.votedFor = candidateID
				rf.persist()
				DPrintf("Term %d: [%d] votes for [%d] in term %d", rf.currentTerm, rf.me, candidateID, rf.currentTerm)
			} else {
				reply.VoteGranted = false
				DPrintf("Term %d: [%d] rejects candidate [%d] in term %d: logs lower", rf.currentTerm, rf.me, candidateID, rf.currentTerm)
			}
		} else {
			reply.VoteGranted = false
			DPrintf("Term %d: [%d] rejects candidate [%d] in term %d: have voted [%d]", rf.currentTerm, rf.me, candidateID, rf.currentTerm, rf.votedFor)
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

//
func (rf *Raft) CallRequestVote(server int) bool {
	rf.mu.Lock()
	length := len(rf.logs)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: length - 1,
		LastLogTerm:  rf.logs[length-1].Term,
	}
	rf.mu.Unlock()

	var reply RequestVoteReply

	// if server is disconnected, sendRequestVote() will be blocked
	// ok := rf.sendRequestVote(server, &args, &reply)
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		// DPrintf("Term %d: [%d] fails to call request vote RPC on [%d]", rf.currentTerm, rf.me, server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("Term %d: [%d] state: %v, receive vote reply from [%d], reply.term = %d, voted: %v", rf.currentTerm, rf.me, rf.state, server, reply.Term, reply.VoteGranted)

	// allow outdated reply to update state, otherwise it is hard to elect a leader in time
	if rf.state != CANDIDATE {
		return false
	} else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		DPrintf("Term %d: [%d] state transform: candidate -> follower, term update: %v", rf.currentTerm, rf.me, reply.Term)
	}

	return reply.VoteGranted
}

func (rf *Raft) AttemptElection() bool {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("Term %d: [%d] attempting an election at term [%d]", rf.currentTerm, rf.me, rf.currentTerm)
	rf.mu.Unlock()

	count := 1 // get default vote from self
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for server, _ := range rf.peers {
		// needn't send RPC to self, send RPCs to other servers
		if server == rf.me {
			continue
		}
		go func(server int) {
			VoteGranted := rf.CallRequestVote(server)
			mu.Lock()
			defer mu.Unlock()
			if VoteGranted {
				count++
			}
			finished++
			cond.Broadcast()
		}(server)
	}

	// collect votes
	mu.Lock()
	for count < rf.majorNum && finished != rf.serversNum {
		cond.Wait()
	}
	mu.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Once a candidate gets enough votes, become a leader
	if count >= rf.majorNum {
		// optimization: append a log (no content) when becoming a leader
		if OPTIMIZATION {
			logentry := LogEntry{Term: rf.currentTerm, Command: -1}
			rf.logs = append(rf.logs, logentry)
			rf.persist()
		}

		// initialization
		for i := 0; i < rf.serversNum; i++ {
			rf.nextIndex[i] = len(rf.logs)
			if i == rf.me {
				rf.matchIndex[i] = len(rf.logs) - 1
				rf.lastAppliedIndex[i] = len(rf.logs) - 1
			} else {
				rf.matchIndex[i] = 0
				rf.lastAppliedIndex[i] = 0
			}
		}

		rf.isLeaderChan <- true

		DPrintf("Term %d: [%d] state transform: candidate -> leader", rf.currentTerm, rf.me)
	}

	// DPrintf("Term %d: [%d] finish election %d, safely exits", rf.currentTerm, rf.me, curRound)
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
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader = rf.GetState()
	if !rf.killed() && isLeader {
		index = len(rf.logs)
		term = rf.currentTerm
		rf.matchIndex[rf.me] = index
		rf.lastAppliedIndex[rf.me] = index
		logentry := LogEntry{Term: term, Command: command}
		rf.logs = append(rf.logs, logentry)
		rf.persist()
		DPrintf("Term %d: [%d] appends new log, term: %v, command: %v", rf.currentTerm, rf.me, logentry.Term, logentry.Command)
	}

	return index, term, isLeader
}

func (rf *Raft) CommitLogs() {

	// select a max index which has been replicated in major servers
	selectMajor := func(array []int) int {
		max_logIndex := -1
		available_logIndex := -1
		for i := 0; i < rf.serversNum; i++ {
			if array[i] > max_logIndex {
				max_logIndex = array[i]
			}
		}

		for j := max_logIndex; j >= rf.lastApplied; j-- {
			count := 0
			for i := 0; i < rf.serversNum; i++ {
				if array[i] >= j {
					count++
				}
			}
			if count >= rf.majorNum {
				available_logIndex = j
				break
			}
		}

		return available_logIndex
	}

	// commit logs[start+1:end]
	commitLogs := func(start int, end int, logs []LogEntry) {
		// DPrintf("Term %d: [%d] all logs: %v", rf.currentTerm, rf.me, logs)
		DPrintf("Term %d: [%d] commits logs: %d -> %d", rf.currentTerm, rf.me, start, end)
		for i := start; i <= end; i++ {
			commitLog := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			rf.applyCh <- commitLog
			DPrintf("Term %d: [%d] commits log, id: %d, content: %v", rf.currentTerm, rf.me, i, commitLog)
		}
	}

	rf.mu.Lock()

	if rf.state == LEADER {
		// update leader's commitIndex
		// restriction: only logs appended in current term can be committed
		commitIndex := selectMajor(rf.matchIndex)
		if commitIndex > rf.commitIndex && rf.logs[commitIndex].Term == rf.currentTerm {
			DPrintf("Term %d: [%d] updates commitIndex, %v -> %v", rf.currentTerm, rf.me, rf.commitIndex, commitIndex)
			rf.commitIndex = commitIndex
			rf.persist()
		}

		// update leader's availableAppliedIndex
		appliedIndex := selectMajor(rf.lastAppliedIndex)
		if appliedIndex > rf.availableAppliedIndex {
			DPrintf("Term %d: [%d] updates availableAppliedIndex, %v -> %v", rf.currentTerm, rf.me, rf.availableAppliedIndex, appliedIndex)
			rf.availableAppliedIndex = appliedIndex
			rf.persist()
		}
	}

	start := rf.lastApplied + 1
	end := rf.availableAppliedIndex
	logs := rf.logs
	rf.mu.Unlock()

	if end >= start {
		// committing without holding a lock, because applyCh may be blocked
		commitLogs(start, end, logs)
		rf.mu.Lock()
		rf.lastApplied = end
		rf.mu.Unlock()
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
	return time.Duration(rand.Intn(MAX_ADDITIONALE_ELECTION_TIMEOUT)+BASE_ELECTION_TIMEOUT) * time.Millisecond
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{Term: -1, Command: -1}) // fill the blank
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.serversNum)
	rf.matchIndex = make([]int, rf.serversNum)
	rf.applyCh = applyCh

	rf.state = FOLLOWER
	rf.HeartBeatChan = make(chan int)
	rf.RequestVoteChan = make(chan int)
	rf.isLeaderChan = make(chan bool)
	rf.lastAppliedIndex = make([]int, rf.serversNum)
	rf.availableAppliedIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	reset := make(chan bool)

	var timer *time.Timer
	isTimerStop := false

	// background goroutine 1: server operation
	go func() {
		for {
			_, isleader := rf.GetState()
			if !isleader {
				// DPrintf("Term %d: [%d] is leader? %v", rf.currentTerm, rf.me, isleader)
				timer = time.NewTimer(GetRandTimeOut())
				isTimerStop = false

				// 1. receive heartbeat signal -> reset the timer
				// 2. receive request vote signal -> reset the timer
				// 3. reach a time out -> kick off a election
				// 4. receive a isleader signal -> reset the timer and later remove it
				select {
				// case leaderID := <-rf.HeartBeatChan:
				case <-rf.HeartBeatChan:
					// DPrintf("Term %d: [%d] receives a heartbeat from [%d] in term %d", rf.currentTerm, rf.me, leaderID, rf.currentTerm)
					reset <- true
				// case candidateID := <-rf.RequestVoteChan:
				case <-rf.RequestVoteChan:
					// DPrintf("Term %d: [%d] receives a request vote from [%d] in term %d", rf.currentTerm, rf.me, candidateID, rf.currentTerm)
					reset <- true
				case <-timer.C:
					DPrintf("Term %d: [%d] time out", rf.currentTerm, rf.me)
					reset <- true
					if !rf.killed() {
						go rf.AttemptElection()
					}
				case isleader = <-rf.isLeaderChan:
					DPrintf("Term %d: [%d] becomes a leader", rf.currentTerm, rf.me)
					rf.state = LEADER
					reset <- true
				}
			} else {
				// leader doesn't need timer
				if !isTimerStop {
					timer.Stop()
					isTimerStop = true
				}

				if !rf.killed() {
					rf.SpreadHeartBeat()
				}

				time.Sleep(time.Duration(HEARTBEAT_INTERNEL) * time.Millisecond)
			}
		}
	}()

	// background goroutine 2: reset timer
	go func() {
		for {
			select {
			case <-reset:
				// DPrintf("Term %d: [%d] reset timer", rf.currentTerm, rf.me)
				timer.Reset(GetRandTimeOut())
			}
		}
	}()

	// background goroutine 3: commit logs
	go func() {
		for {
			if !rf.killed() {
				rf.CommitLogs()
			}
			time.Sleep(time.Duration(COMMIT_INTERNEL) * time.Millisecond)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
