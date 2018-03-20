package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = ke(...)
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

import "fmt"
import "math/rand"
import "sync"
import "sync/atomic"
import "time"
import "github.com/6.824/labrpc"

import "bytes"
import "encoding/gob"

// the status of raft peer.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// the event types
type Event int

const (
	Timeout Event = iota
	// HeartBeat Event = iota
	NewTerm
	Win
	Stop
)

// handler to make agreement
type Processor interface {
	process(server int)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to ke().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	serverMu    sync.RWMutex
	isStopping  bool
	state       State
	currentTerm int
	voteFor     int        // index of peer
	log         []LogEntry // using slices

	commitIndex int
	lastApplied int

	// timer
	electionTimeout time.Duration
	electionTimer   *time.Timer

	// only for leader
	nextIndex  map[int]int // peer id -> appliedIndex
	matchIndex map[int]int // peer id -> highest index

	// event channel
	heartbeatChan chan struct{}
	// state channel
	stateChan chan State
	// apply channel
	applyChan chan ApplyMsg

	// max attempts
	maxAttempts int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.voteFor)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.logs)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.voteFor)
	// d.Decode(&rf.currentTerm)
	// d.Decode(&rf.logs)
}

// AppendEntry RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int
	VoteGrant bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

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
	ok := rf.callWithRetry(server, "Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) callWithRetry(server int, method string, args interface{}, reply interface{}) bool {
	ok := false
	for time := 1; !ok && time < rf.maxAttempts; time++ {
		ok := rf.peers[server].Call(method, args, reply)
	}
	return ok
}

// transition state
func (rf *Raft) transitionState(currentState State, event Event) (nextState State) {
	var nextState State = Follower // default is Follower
	switch event {
	case Timeout:
		if currentState == Follower {
			nextState = Candidate
		} else if currentState == Candidate {
			nextState = Candidate
		} // leader should not have Timeout event.
	case NewTerm:
		if currentState == Candidate {
			nextState = Follower
		} else if currentState == Leader {
			nextState = Follower
		} // if follower get NewTerm, it should stay in state: follower.
	case Win:
		if currentState == Candidate {
			nextState = Leader
		}
	}
	return nextState
}

//
func (rf *Raft) electionService() {
	for {
		switch currentState {
		case Follower:
			select {
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				nextState := rf.transitionState(rf.state, Timeout)
				rf.state = nextState
				if nextState == Candidate {
					rf.currentTerm += 1
				}
				rf.mu.Unlock()
				DPrintf("LSM has set state to candidate.\n")
			case <-rf.heartbeatChan:
				DPrintf("Received heartbeat from leader, reset timer.\n")
			}
		case Candidate:
			// start a election.
			process := func(server int) bool {
				request := rf.createVoteRequest()
				reply := new(RequestVoteReply)
				rf.sendRequestVote(server, request, reply)
				rf.processVoteReply(reply)
			}
			ok := rf.agreeWithServers(process)
			if ok {
				rf.mu.Lock()
				rf.transitionState(rf.state, rf.Win)
				rf.mu.Unlock()
			}
		case Leader:
			// start to send heartbeat.
		default:
		}
	}
}

func (rf *Raft) ceateVoteRequest() *RequestVoteArgs {
	request := new(RequestVoteArgs)
	rf.mu.Lock()
	request.Term = rf.currentTerm
	request.Candidate = rf.me
	logIndex = len(rf.logs) - 1
	rf.mu.Unlock()
	if logIndex < 1 {
		logIndex = 0
	}
	request.LastLogTerm = rf.log[logIndex].Term
	request.LostLogIndex = logIndex
	return request
}

func (rf *Raft) processVoteReply(reply *RequestVoteReply) (win bool) {
	win := false
	if reply != nil {
		if reply.VoteGrant {
			win = true
		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.transitionState(rf.state, rf.NewTerm)
			rf.mu.Unlock()
		}
	}
	return win
}

func (rf *Raft) agreeWithServers(process func(server int) bool) (agree bool) {
	doneChan := make(chan int)
	for i, peer := range rf.peers {
		go func(server int) {
			ok := process(server)
			if ok {
				doneChan <- server
			}
		}(peer)
	}
	deadline := time.After(rf.electionTimeout)
	doneCount := 0
	for {
		select {
		case <-deadline:
			DPrintf("Agreement timeout!\n")
			return false
		case server := <-doneChan:
			doneCount += 1
			if doneCount >= len(rf.peers)+1 {
				return true
			}
		}
	}
}

func (rf *Raft) createAppendEntriesRequest(start int, stop int, term int, isHeartbeat bool) *AppendEntriesArgs {
	currentLen := len(rf.logs)
	if start < 0 || stop > start || (start == currentLen && !isHeartbeat) {
		fmt.Printf()
		return nil
	}
	if stop > currentLen {
		stop = currentLen
	}
	request := new(AppendEntriesArgs)
	request.Term = term
	request.LeaderId = rf.me
	request.LeaderCommit = rf.commitIndex
	prevLogIndex := start - 1
}

func (rf *Raft) sendHeartbeat() bool {

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

func (rf *Raft) sleep(elapse int) {
	if elapse > 0 {
		time.Sleep(time.Duration(elapse) * time.Millisecond)
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// ke() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.isVoting = true
	rf.isStopping = false
	rf.isLeader = false
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.syncLogs = make(map[int]int)
	atomic.StoreInt64(&rf.lastTick, time.Now().UnixNano())

	// init nextIndex and matchIndex
	logSize := len(rf.logs)
	for key := 0; key < len(rf.peers); key++ {
		rf.nextIndex[key] = logSize
		rf.matchIndex[key] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
