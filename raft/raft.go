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

import (
	"bytes"
	"encoding/gob"
	"github.com/6.824/labrpc"
	"math/rand"
	"sync"
	"time"
)

// the status of raft peer.
type State int

const (
	Follower State = iota
	Candidate
	Leader
	End
)

// the event types
type Event int

const (
	Timeout Event = iota
	HeartBeat
	NewTerm
	NewLeader
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

type SyncMsg struct {
	Server int
	Index  int
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
	state       State
	currentTerm int
	voteFor     int        // index of peer
	log         []LogEntry // using slices

	commitIndex int
	lastApplied int

	// timer
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	// only for leader
	nextIndex  map[int]int // peer id -> appliedIndex
	matchIndex map[int]int // peer id -> highest index

	// event channel
	heartbeatChan chan string
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.log)
}

// ======== Part: Vote ===========
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args == nil {
		DPrintf("Peer-%d received a null vote request.", rf.me)
		return
	}
	candidateTerm := args.Term
	candidateId := args.Candidate
	DPrintf("Peer-%d received a vote request %v from peer-%d.", rf.me, *args, candidateId)
	if candidateTerm < currentTerm {
		DPrintf("Peer-%d's term=%d > candidate's term=%d.\n", rf.me, currentTerm, candidateTerm)
		reply.Term = currentTerm
		reply.VoteGrant = false
		return
	} else if candidateTerm == currentTerm {
		if rf.voteFor != -1 && rf.voteFor != candidateId {
			DPrintf("Peer-%d has grant to peer-%d before this request from peer-%d.", rf.me, rf.voteFor, candidateId)
			reply.Term = currentTerm
			reply.VoteGrant = false
			return
		}
		DPrintf("Peer-%d's term=%d == candidate's term=%d, to check index.\n", rf.me, currentTerm, candidateTerm)
	} else {
		DPrintf("Peer-%d's term=%d < candidate's term=%d.\n", rf.me, currentTerm, candidateTerm)
		// begin to update status
		rf.currentTerm = candidateTerm // find larger term, up to date
		rf.transitionState(NewTerm)    // transition to Follower.
	}
	// check whose log is up-to-date
	candiLastLogIndex := args.LastLogIndex
	candiLastLogTerm := args.LastLogTerm
	localLastLogIndex := len(rf.log) - 1
	localLastLogTerm := -1
	if localLastLogIndex >= 0 {
		localLastLogTerm = rf.log[localLastLogIndex].Term
	}
	// check term first, if term is the same, then check the index.
	DPrintf("Peer-%d try to check last entry, loacl: index=%d;term=%d, candi: index=%d,term=%d.", rf.me, localLastLogIndex, localLastLogTerm, candiLastLogIndex, candiLastLogTerm)
	if localLastLogTerm > candiLastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		return
	} else if localLastLogTerm == candiLastLogTerm {
		if localLastLogIndex > candiLastLogIndex {
			reply.Term = rf.currentTerm
			reply.VoteGrant = false
			return
		}
	} else {
	}
	// heartbeat.
	go func() {
		rf.heartbeatChan <- "hb"
	}()
	// local log are up-to-date, grant
	// before grant to candidate, we should reset ourselves state.
	rf.transitionState(NewLeader)
	rf.voteFor = candidateId
	reply.Term = rf.currentTerm
	reply.VoteGrant = true
	DPrintf("Peer-%d grant to peer-%d.", rf.me, candidateId)
	rf.persist()
	return
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

func (rf *Raft) createVoteRequest() *RequestVoteArgs {
	request := new(RequestVoteArgs)
	rf.mu.Lock()
	request.Term = rf.currentTerm
	request.Candidate = rf.me
	logIndex := len(rf.log) - 1
	rf.mu.Unlock()
	if logIndex < 1 {
		logIndex = 0
	}
	request.LastLogTerm = rf.log[logIndex].Term
	request.LastLogIndex = logIndex
	return request
}

func (rf *Raft) processVoteReply(reply *RequestVoteReply) (win bool) {
	win = false
	if reply != nil {
		if reply.VoteGrant {
			win = true
		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.transitionState(NewTerm)
			rf.mu.Unlock()
		}
	}
	return win
}

// ======= Part: AppendEntry ======

// AppendEntries RPC
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
	PeerId       int
}

// TODO: extract subfunction from the function.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	localTerm := rf.currentTerm
	logSize := len(rf.log)
	// init reply. Term to localTerm
	reply.Term = localTerm
	reply.PeerId = rf.me
	reply.Success = false
	reply.ConflictTerm = -1
	reply.FirstIndex = -1
	// begin to check.
	// 1. check term.
	DPrintf("Peer-%d has reveived new append request: %v, local: term=%d.", rf.me, *args, localTerm)
	if localTerm > args.Term {
		reply.Success = false
		return
	} else if localTerm < args.Term {
		rf.currentTerm = args.Term
		rf.transitionState(NewTerm)
	}
	// 2. process heartbeat.
	appendEntriesLen := 0
	if args.Entries != nil {
		appendEntriesLen = len(args.Entries)
	}
	// localTerm <= args.Term, it should receive heartbeat.
	if appendEntriesLen <= 0 || args.Entries[0].Command == nil {
		// when receive heartbeat, we should turn from Canditate to Follower.
		rf.transitionState(HeartBeat)
		rf.voteFor = args.LeaderId
		DPrintf("Peer-%d try to send heartbeat message.", rf.me)
		// to send msg should void deadlock:
		// A -> B.AppendEntries, B hold the lock and send msg;
		// B.electionService, B try to hold lock to process, if not, it wait, so can not receive msg.
		// send message to heartbeat channel.
		go func() {
			rf.heartbeatChan <- "hb"
		}()
		DPrintf("Peer-%d received heartbeat from peer-%d.", rf.me, args.LeaderId)
	}
	// 3. the term is the same, check term of the previous log.
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	// 3.1. check arguments.
	if prevLogTerm < 0 || prevLogIndex < 0 || prevLogIndex >= logSize {
		reply.Success = false
		if prevLogIndex >= logSize && logSize > 0 {
			// if the leader's log are more than follower's
			reply.FirstIndex = logSize
			// reply.ConflictTerm = rf.log[logSize-1].Term
		}
		return
	}
	// 3.2. check previous log's term.
	localPrevLogTerm := rf.log[prevLogIndex].Term
	DPrintf("Peer-%d local: prevLogTerm=%d, prevLogIndex=%d.", rf.me, localPrevLogTerm, prevLogIndex)
	if prevLogTerm != localPrevLogTerm {
		reply.Success = false
		// to find the first index of conflict term.
		conflictTerm := localPrevLogTerm
		reply.ConflictTerm = conflictTerm
		// TODO: replace this loop with binary search.
		// The lower boundary is the commintIndex, because all the entries below commitIndex have been commit.
		for i := prevLogIndex; i >= rf.commitIndex; i-- {
			if rf.log[i].Term != conflictTerm {
				reply.FirstIndex = i + 1
				break
			}
		}
		if reply.FirstIndex == -1 {
			reply.FirstIndex = rf.commitIndex + 1
		}
		return
	}
	// 4. the previous log's term is the same, we can update commitIndex and append log now.
	// 4.1. update commit index.
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("Peer-%d set commitIndex=%d, origin=%d, from leader-%d.", rf.me, args.LeaderCommit, rf.commitIndex, args.LeaderId)
		rf.commitIndex = args.LeaderCommit
	}
	// 5. begin to append log.
	// 5.1. to find the same log between local log and args log.
	firstDiffLogPos := -1
	appendPos := prevLogIndex + 1
	if appendEntriesLen > 0 {
		for argsLogIndex, appendEntry := range args.Entries {
			// localLogsIndex points to local log, its start position is prevLogIndex + 1;
			// argsLogIndex points to args' log entries, start from 0;
			// compaire local log and args log one by one.
			if appendPos < logSize && rf.log[appendPos].Term == appendEntry.Term {
				appendPos += 1 // move local log' pointer to next one.
				continue
			} else {
				firstDiffLogPos = argsLogIndex
				break
			}
		}
	}
	// 5.2. do append.
	if firstDiffLogPos != -1 {
		// cut log to position=appendPos - 1
		if appendPos > 0 {
			rf.log = rf.log[0:appendPos]
		}
		// append the different part of args.Entries to log.
		rf.log = append(rf.log, args.Entries[firstDiffLogPos:]...)
		DPrintf("Peer-%d append entries to log, log' length=%d, log=%v\n", rf.me, len(rf.log), rf.log)
	} else {
		if appendEntriesLen > 0 {
			DPrintf("Peer-%d do not append duplicate log.\n", rf.me)
		}
	}
	// 6. reply.
	reply.Term = localTerm
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.callWithRetry(server, "Raft.AppendEntries", args, reply)
	return ok
}

// hold the lock outside.
func (rf *Raft) createAppendEntriesRequest(start int, stop int, term int) *AppendEntriesArgs {
	DPrintf("Peer-%d create AppendEntriesRequest with start=%d, stop=%d, term=%d.", rf.me, start, stop, term)
	currentLen := len(rf.log)
	if start < 0 || stop <= start {
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
	if prevLogIndex >= 0 && prevLogIndex < currentLen {
		request.PrevLogIndex = prevLogIndex
		request.PrevLogTerm = rf.log[prevLogIndex].Term
	} else {
		request.PrevLogIndex = 0
		request.PrevLogTerm = 0
	}
	if start < currentLen && stop >= start {
		if start == 0 {
			start = 1
		}
		request.Entries = rf.log[start:stop]
	}
	DPrintf("Peer-%d create an appendRequest: %v", rf.me, *request)
	return request
}

func (rf *Raft) processAppendEntriesReply(nextLogIndex int, reply *AppendEntriesReply) (succ bool) {
	succ = false
	if reply != nil {
		succ = reply.Success
		server := reply.PeerId
		DPrintf("Peer-%d process AppendEntriesReply=%v.", rf.me, *reply)
		rf.mu.Lock()
		if succ {
			if nextLogIndex >= rf.nextIndex[server] {
				DPrintf("Peer-%d set nextIndex=%d, origin=%d", rf.me, nextLogIndex, rf.nextIndex[server])
				rf.nextIndex[server] = nextLogIndex
			}
			if nextLogIndex-1 >= rf.matchIndex[server] {
				DPrintf("Peer-%d set matchIndex=%d, origin=%d", rf.me, nextLogIndex-1, rf.matchIndex[server])
				rf.matchIndex[server] = nextLogIndex - 1
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.transitionState(NewTerm)
				DPrintf("Peer-%d change term %d -> %d, state leader -> %v.", rf.me, rf.currentTerm, reply.Term, rf.state)
			}
			if reply.FirstIndex >= 0 {
				if rf.log[reply.FirstIndex].Term != reply.ConflictTerm {
					DPrintf("Peer-%d set nextIndex[%d]=%d.", rf.me, reply.PeerId, reply.FirstIndex)
					rf.nextIndex[reply.PeerId] = reply.FirstIndex
				}
			}
		}
		rf.mu.Unlock()
	}
	return succ
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	currentIndex := len(rf.log)
	currentTerm := rf.currentTerm
	request := rf.createAppendEntriesRequest(currentIndex, currentIndex+1, currentTerm)
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			DPrintf("Peer-%d begin to send heartbeat to peer-%d.", rf.me, server)
			reply := new(AppendEntriesReply)
			ok := rf.sendAppendEntries(server, request, reply)
			if ok {
				DPrintf("Peer-%d has sent heartbeat to peer-%d.", rf.me, server)
			} else {
				DPrintf("Peer-%d sent heartbeat to peer-%d failed.", rf.me, server)
			}
		}(index)
	}
}

// ======= Part: Service =========
// Append service.
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
	if rf.state == Leader {
		newLogEntry := LogEntry{}
		rf.mu.Lock()
		if rf.state == Leader {
			term = rf.currentTerm
			newLogEntry.Term = term
			newLogEntry.Command = command
			rf.log = append(rf.log, newLogEntry)
			index = len(rf.log) - 1
			// update leader's matchIndex and nextIndex
			rf.matchIndex[rf.me] = index
			rf.nextIndex[rf.me] = index + 1
			rf.persist()
		} else {
			DPrintf("Peer-%d, before lock, the state has changed to %d.\n", rf.me, rf.state)
		}
		if term != -1 {
			DPrintf("Peer-%d start to append %v to peers.\n", rf.me, command)
			request := rf.createAppendEntriesRequest(index, index+1, term)
			appendProcess := func(server int) bool {
				reply := new(AppendEntriesReply)
				rf.sendAppendEntries(server, request, reply)
				ok := rf.processAppendEntriesReply(index+1, reply)
				if ok {
					DPrintf("Peer-%d append log=%v to peer-%d successfully.\n", rf.me, request.Entries, server)
				} else {
					DPrintf("Peer-%d append log=%v to peer-%d failed.\n", rf.me, request.Entries, server)
				}
				return ok
			}
			go func() {
				ok := rf.agreeWithServers(appendProcess)
				if ok {
					// if append successfully, update commit index.
					rf.mu.Lock()
					if index >= rf.commitIndex {
						DPrintf("Peer-%d set commit=%d, origin=%d.", rf.me, index, rf.commitIndex)
						rf.commitIndex = index
					} else {
						DPrintf("Peer-%d get a currentIndex=%d < commitIndex=%d, it can not be happend.", rf.me, index, rf.commitIndex)
					}
					rf.mu.Unlock()
				} else {
					DPrintf("Peer-%d start agreement with servers failed. currentIndex=%d.\n", rf.me, index)
				}
			}()
		}
		rf.mu.Unlock()
	} else {
		isLeader = false
	}
	return index, term, isLeader
}

// Election service: to elect leader.
func (rf *Raft) electionService() {
	for {
		rf.mu.Lock()
		// snapshot current state of raft, (state & term)
		currentState := rf.state
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		switch currentState {
		case Follower:
			// clear raft state.
			select {
			case <-time.After(rf.electionTimeout + time.Duration(rand.Intn(500))):
				DPrintf("Peer-%d's election is timeout.", rf.me)
				rf.mu.Lock()
				DPrintf("Peer-%d's election hold the lock, now the currtentTerm=%d, rf.currentTerm=%d.", rf.me, currentTerm, rf.currentTerm)
				// we should record the currentTerm for which the timer wait.
				if rf.state == Follower && rf.currentTerm == currentTerm {
					rf.transitionState(Timeout)
					DPrintf("Peer-%d LSM has set state to candidate.", rf.me)
				}
				rf.mu.Unlock()
				DPrintf("Peer-%d turn state from %v to %v.", rf.me, currentState, rf.state)
			case <-rf.heartbeatChan:
				DPrintf("Peer-%d Received heartbeat from leader, reset timer.", rf.me)
			}
		case Candidate:
			// start a election.
			// using a thread to do election, and sending message to channel;
			// this can let the heartbeat to break the election progress.
			voteDoneChan := make(chan bool)
			go func() {
				TPrintf("Peer-%d becomes candidate, try to hold the lock.", rf.me)
				rf.mu.Lock()
				TPrintf("Peer-%d becomes candidate, has hold the lock, rf.voteFor=%d.", rf.me, rf.voteFor)
				// check state first, if the state has changed, do not vote.
				// then check voteFor, if it has voteFor other peer in this term.
				toVote := rf.state == Candidate && (rf.voteFor == -1 || rf.voteFor == rf.me)
				if toVote {
					rf.voteFor = rf.me // should mark its voteFor when it begins to vote.
					DPrintf("Peer-%d set voteFor=%d.", rf.me, rf.voteFor)
				}
				rf.mu.Unlock()
				ok := false
				if toVote {
					DPrintf("Peer-%d begin to vote.", rf.me)
					request := rf.createVoteRequest()
					// the process logic for each peer.
					process := func(server int) bool {
						reply := new(RequestVoteReply)
						rf.sendRequestVote(server, request, reply)
						ok := rf.processVoteReply(reply)
						return ok
					}
					ok = rf.agreeWithServers(process)
				}
				voteDoneChan <- ok
			}()
			select {
			case done := <-voteDoneChan:
				if done {
					DPrintf("Peer-%d win.", rf.me)
					// if voting is success, we set state to leader.
					rf.mu.Lock()
					if rf.state == Candidate {
						rf.transitionState(Win)
					}
					rf.mu.Unlock()
					DPrintf("Peer-%d becomes the leader.", rf.me)
				} else {
					// if voting is failed, we reset voteFor to -1, but do not reset state and term.
					rf.mu.Lock()
					if rf.state == Candidate {
						rf.transitionState(Timeout)
					}
					rf.mu.Unlock()
					sleep(rand.Intn(500))
				}
			case <-rf.heartbeatChan:
				// if another is win, we will receive heartbeat, so we shoul
				DPrintf("Peer-%d received heartbeat when voting, turn to follower, reset timer.", rf.me)
				rf.mu.Lock()
				rf.transitionState(NewTerm)
				rf.mu.Unlock()
			}
		case Leader:
			// start to send heartbeat.
			DPrintf("Peer-%d try to send heartbeat.", rf.me)
			rf.sendHeartbeat()
			time.Sleep(rf.heartbeatInterval)
		case End:
			DPrintf("Peer-%d is stopping.\n", rf.me)
			return
		default:
			DPrintf("Do not support state: %v\n", currentState)
		}
	}
}

// Agreement service: agreement with timeout.
func (rf *Raft) agreeWithServers(process func(server int) bool) (agree bool) {
	doneChan := make(chan int)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			ok := process(server)
			if ok {
				doneChan <- server
			}
		}(i)
	}
	deadline := time.After(rf.electionTimeout)
	doneCount := 1
	peerCount := len(rf.peers)
	for {
		select {
		case <-deadline:
			DPrintf("Peer-%d, agreement timeout!\n", rf.me)
			return false
		case server := <-doneChan:
			if server >= 0 && server < peerCount {
				doneCount += 1
				if doneCount >= peerCount/2+1 {
					DPrintf("Peer-%d's agreement is successful.", rf.me)
					return true
				}
			} else {
				DPrintf("Peer-%d find an illegal server number=%d when do agreement.", rf.me, server)
			}
		}
	}
}

// apply service
func (rf *Raft) applyService() {
	DPrintf("Peer-%d start applyService.\n", rf.me)
	currentIndex := 0
	for rf.state != End {
		toApply := false
		rf.mu.Lock()
		logSize := len(rf.log)
		currentIndex = rf.lastApplied + 1
		commitIndex := rf.commitIndex
		if currentIndex > 0 && currentIndex <= commitIndex {
			toApply = true
		}
		rf.mu.Unlock()
		if !toApply {
			DPrintf("Peer-%d do not apply log, currentIndex=%d, commitIndex=%d, logSize=%d.", rf.me, currentIndex, commitIndex, logSize)
			sleep(100) // if the raft has no new agreement, this check will always occupy this thread.
			continue
		}
		if currentIndex >= logSize {
			// this should not happen.
			// TODO: throw exception here, panic
			DPrintf("Peer-%d, currentIndex=%d >= logSize=%d.\n", rf.me, currentIndex, logSize)
			rf.mu.Lock()
			rf.transitionState(Stop)
			rf.mu.Unlock()
			break
		}
		// do apply.
		msg := ApplyMsg{}
		msg.Index = currentIndex
		msg.Command = rf.log[currentIndex].Command
		rf.applyChan <- msg
		rf.lastApplied = currentIndex
		DPrintf("Peer-%d has applied msg={%v}.\n", rf.me, msg)
	}
}

// log sync service
func (rf *Raft) logSyncService() {
	DPrintf("Peer-%d start logSyncService.\n", rf.me)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			for rf.state != End {
				rf.mu.Lock()
				lastLogIndex := len(rf.log)
				nextLogIndex := rf.nextIndex[server]
				currentTerm := rf.currentTerm
				currentState := rf.state
				rf.mu.Unlock()
				if currentState != Leader {
					// only the leader can sync log!!!
					DPrintf("Peer-%d is not leader now, do not sync log to peer-%d", rf.me, server)
					time.Sleep(rf.electionTimeout)
					continue
				}
				DPrintf("Peer-%d try to sync log to peer-%d, lastLogIndex=%d, nextLogIndex=%d.", rf.me, server, lastLogIndex, nextLogIndex)
				if lastLogIndex > nextLogIndex {
					request := rf.createAppendEntriesRequest(nextLogIndex, lastLogIndex, currentTerm)
					if request == nil {
						DPrintf("Peer-%d create a null request.", rf.me)
						sleep(100)
						continue
					}
					reply := new(AppendEntriesReply)
					ok := rf.sendAppendEntries(server, request, reply)
					if ok {
						DPrintf("Peer-%d: the RPC to synchronize log to peer-%d successfully.\n", rf.me, server)
						succ := rf.processAppendEntriesReply(lastLogIndex, reply)
						if succ {
							DPrintf("Peer-%d: synchronize log to peer-%d successfully.\n", rf.me, server)
							// if it sync log successfully, to update the commit index.
							rf.mu.Lock()
							// it should skip self, because leader updates other followers only,
							// so its matchIndex is always smaller than followers,
							// the minMatchIndex is always filled by leader's matchIndex.
							// it is wrong.
							minMatchIndex := getMinOfMajority(rf.matchIndex, rf.commitIndex)
							if minMatchIndex != 10000 {
								// see the last one of raft rules for leader.
								if minMatchIndex < len(rf.log) && rf.log[minMatchIndex].Term == rf.currentTerm {
									DPrintf("Peer-%d set commitIndex=%d, origin=%d, matchIndex=%v.", rf.me, minMatchIndex, rf.commitIndex, rf.matchIndex)
									rf.commitIndex = minMatchIndex
								}
							}
							rf.mu.Unlock()
						} else {
							DPrintf("Peer-%d: synchronize log to peer-%d failed.\n", rf.me, server)
							sleep(100)
						}
					} else {
						DPrintf("Peer-%d: the RPC to synchronize log to peer-%d failed.\n", rf.me, server)
						sleep(300)
					}
				} else {
					DPrintf("Peer-%d: lastLogIndex <= nextLogIndex, sleep 300ms", rf.me)
					sleep(300)
				}
			}
		}(index)
	}
}

// ======= Part: Utilities =======
// get the min index
func getMinOfMajority(index map[int]int, commitIndex int) (min int) {
	min = 10000
	countMap := make(map[int]int)
	for _, currentIndex := range index {
		_, ok := countMap[currentIndex]
		if !ok {
			countMap[currentIndex] = 0
		}
		// update every entry in countMap, if the key of entry is larger than currentIndex, its value plus one.
		for key, value := range countMap {
			if key <= currentIndex {
				countMap[key] = value + 1
			}
		}
	}
	for key, value := range countMap {
		if min > key && value >= len(index)/2+1 && key > commitIndex {
			min = key
		}
	}
	TPrintf("getMinOfMajority: countMap=%v, commitIndex=%d, min=%d.", countMap, commitIndex, min)
	return min
}

// check state
func checkState(target State, source State) bool {
	return target == source
}

// transition state
func (rf *Raft) transitionState(event Event) (nextState State) {
	currentState := rf.state
	nextState = currentState
	switch event {
	case Timeout:
		if currentState == Follower || currentState == Candidate {
			// the process logic of Follower and Candidate is the same
			// if state is Follower, when it is timeout, to start a new election, so we should increment term
			// if state is Candidate, when it is timeout, to restart a new election, so should increment term
			nextState = Candidate
			// Follower -> Candidate, get new term.
			rf.currentTerm += 1
			rf.voteFor = -1
		} // leader should not have Timeout event.
	case NewTerm:
		if currentState == Candidate {
			nextState = Follower
		} else if currentState == Leader {
			nextState = Follower
		} // if follower get NewTerm, it should stay in state: follower.
		// reset
		rf.voteFor = -1
	case NewLeader:
		if currentState == Candidate {
			nextState = Follower
			rf.voteFor = -1
		}
	case Win:
		if currentState == Candidate {
			nextState = Leader
		}
	case HeartBeat:
		if currentState == Candidate {
			nextState = Follower
		} else if currentState == Leader {
			DPrintf("When current state=%v, we received a heartbeat.", currentState)
		}
	case Stop:
		nextState = End
	default:
		DPrintf("Do not support the event: %v\n", event)
	}
	rf.state = nextState
	DPrintf("Peer-%d trun from %d to %d", rf.me, currentState, nextState)
	return nextState
}

func sleep(elapse int) {
	if elapse > 0 {
		time.Sleep(time.Duration(elapse) * time.Millisecond)
	}
}

// Rpc
func (rf *Raft) callWithRetry(server int, method string, args interface{}, reply interface{}) bool {
	ok := false
	for time := 1; !ok && time < rf.maxAttempts; time++ {
		ok = rf.peers[server].Call(method, args, reply)
	}
	return ok
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.transitionState(Stop)
}

// ========= Part: in ========
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
	DPrintf("Peer-%d begins to initialize.\n", rf.me)
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	timeout := RaftElectionTimeout
	rf.heartbeatInterval = time.Duration(timeout / 2)
	rf.electionTimeout = time.Duration(timeout)
	rf.heartbeatChan = make(chan string, 1) // heartbeat should have 1 entry for message, this can void deadlock.
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.maxAttempts = 3

	// init nextIndex and matchIndex
	logSize := len(rf.log)
	for key, _ := range rf.peers {
		rf.nextIndex[key] = logSize
		rf.matchIndex[key] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Peer-%d is initialized. log=%v, term=%d, leader=%d\n", rf.me, rf.log, rf.currentTerm, rf.state)

	// start services.
	DPrintf("Peer-%d start services\n", rf.me)
	go rf.electionService()
	go rf.applyService()
	go rf.logSyncService()

	// for rf.state != End {
	// 	sleep(1000)
	// }

	return rf
}
