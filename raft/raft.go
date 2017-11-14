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

// import "bytes"
// import "encoding/gob"

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
	isVoting    bool
	isStopping  bool
	isLeader    bool
	currentTerm int
	voteFor     int        // index of peer
	logs        []LogEntry // using slices

	commitIndex int
	lastApplied int

	// timer
	lastTick int64

	// only for leader
	syncLogs   map[int]bool
	nextIndex  map[int]int // peer id -> appliedIndex
	matchIndex map[int]int // peer id -> highest index

	// apply channel
	applyChan chan ApplyMsg
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
	isleader = rf.isLeader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// AppendEntry RPC
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// LogTerm      int
	// LogCommand   interface{}
	Entry        LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	fmt.Printf("peer-%d receives msg.\n", rf.me)
	//rf.serverMu.RLock()
	//defer rf.serverMu.RUnlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 以下的判断顺序不能随便变动！！
	fmt.Printf("Peer-%d receives msg{term=%d, pidx=%d, pterm=%d, cmt=%d, v=%v} from peer-%d.\n", rf.me, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entry, args.LeaderId)
	/*
		if rf.isVoting {
			fmt.Printf("peer-%d is voting. do not append.\n", rf.me)
			// to reply
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	*/
	// 1.update time.
	if args.Entry.Command == nil {
		atomic.StoreInt64(&rf.lastTick, time.Now().UnixNano())
		fmt.Printf("Receive hearbeat, peer-%d set lastTick to %d\n", rf.me, rf.lastTick)
	}
	// 2.check currentTerm
	localTerm := rf.currentTerm
	if localTerm > args.Term {
		fmt.Printf("Peer-%d term=%d, leader term=%d\n", rf.me, localTerm, args.Term)
		reply.Success = false
		reply.Term = localTerm
		return
	} else if localTerm < args.Term {
		rf.currentTerm = args.Term
	}
	// 3.check index and term of previous log
	if args.PrevLogTerm < 0 || args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		fmt.Printf("Args: PrevLogTerm=%d, PrevLogIndex=%d; local: PrevLogIndex=%d\n", args.PrevLogTerm, args.PrevLogIndex, len(rf.logs)-1)
		reply.Term = localTerm
		reply.Success = false
		return
	}
	// 4.update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}
	// 5.check current log entry.
	index := args.PrevLogIndex + 1
	if index < len(rf.logs) {
		if rf.logs[index].Term == args.Entry.Term {
			fmt.Printf("Peer-%d matched append log at index=%d\n", rf.me, index)
			reply.Term = localTerm
			reply.Success = true
			return
		} else {
			fmt.Printf("args' entry=%d, but local logs[%d]=%d\n", args.Entry.Term, index, rf.logs[index].Term)
			rf.logs = rf.logs[0:index]
		}
	}
	// 6.do append.
	//   check command, if command is nil, it means this request is heartbeat, do not append.
	if args.Entry.Command != nil {
		// newLogEntry := new()
		rf.logs = append(rf.logs, args.Entry)
		fmt.Printf("Peer-%d append entry to logs, logs' length=%d, logs=%v.\n", rf.me, len(rf.logs), rf.logs)
	} else {
		fmt.Printf("Peer-%d do not append heartbeat.\n", rf.me)
	}
	// 7.reply.
	reply.Term = rf.currentTerm
	reply.Success = true
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
	//rf.serverMu.RLock()
	//defer rf.serverMu.RUnlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastTick = time.Now().UnixNano()
	rf.isLeader = false
	currTerm := rf.currentTerm
	if args == nil {
		fmt.Printf("args is null.\n")
		reply.Term = currTerm
		reply.VoteGrant = false
		return
	}
	termOfCandidate := args.Term
	candidateId := args.Candidate
	if termOfCandidate < currTerm {
		reply.Term = currTerm
		reply.VoteGrant = false
		return
	} else if termOfCandidate == currTerm {
		if rf.voteFor != -1 && rf.voteFor != candidateId {
			fmt.Printf("peer-%d has grant to peer-%d in term %d\n", rf.me, rf.voteFor, currTerm)
			reply.Term = -1
			reply.VoteGrant = false
			return
		}
	}
	fmt.Printf("Peer-%d, candidate peer-%d's term %d is larger than my term %d\n", rf.me, candidateId, termOfCandidate, currTerm)
	rf.currentTerm = termOfCandidate
	candiLastLogIndex := args.LastLogIndex
	candiLastLogTerm := args.LastLogTerm
	var localLastLogIndex int = len(rf.logs) - 1
	var localLastLogTerm int = -1
	if localLastLogIndex >= 0 {
		localLastLogTerm = rf.logs[localLastLogIndex].Term
	}
	/*
		if localLastLogIndex > candiLastLogIndex ||
			(localLastLogIndex <= candiLastLogIndex &&
				localLastLogTerm > candiLastLogTerm) {
			reply.Term = -1
			reply.VoteGrant = false
			return
		}
	*/
	// check whose last log is up-to-date.
	if localLastLogTerm > candiLastLogTerm {
		fmt.Printf("Peer-%d's lastLogTerm=%d > peer-%d's lastLogTerm=%d\n", rf.me, localLastLogTerm, candidateId, candiLastLogTerm)
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		return
	} else if localLastLogTerm == candiLastLogTerm {
		fmt.Printf("Peer-%d's lastLogTerm=%d == peer-%d's lastLogTerm=%d\n", rf.me, localLastLogTerm, candidateId, candiLastLogTerm)
		if localLastLogIndex > candiLastLogIndex {
			fmt.Printf("Peer-%d's lastLogIndex=%d > peer-%d's lastLogIndex=%d\n", rf.me, localLastLogIndex, candidateId, candiLastLogIndex)
			reply.Term = rf.currentTerm
			reply.VoteGrant = false
			return
		}
		fmt.Printf("Peer-%d's lastLogIndex=%d <= peer-%d's lastLogIndex=%d\n", rf.me, localLastLogIndex, candidateId, candiLastLogIndex)
	}
	// can grant the request
	rf.voteFor = candidateId
	if rf.isLeader {
		rf.isLeader = false
	}
	reply.Term = currTerm
	reply.VoteGrant = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	fmt.Printf("Is peer-%d the leader? %t\n", rf.me, rf.isLeader)
	if rf.isLeader {
		currIndex := 0
		fmt.Printf("Peer-%d is the leader, starting to make an agreement on command={%v}.\n", rf.me, command)
		// begin to write log to leaders' memory.
		newLogEntry := LogEntry{}
		rf.mu.Lock()
		currTerm := rf.currentTerm
		newLogEntry.Term = currTerm
		newLogEntry.Command = command
		rf.logs = append(rf.logs, newLogEntry)
		currIndex = len(rf.logs) - 1
		rf.mu.Unlock()
		fmt.Printf("Peer-%d has get all data.\n", rf.me)
		// begin to append log to all followers.
		hasCommited := rf.appendToServers(currIndex, currTerm)
		// begin to apply commited command.
		if hasCommited {
			go rf.applyToLocalServiceReplica(currIndex)
		}
		index = currIndex
		term = currTerm
	} else {
		fmt.Printf("Peer-%d is not the leader, return false.\n", rf.me)
		isLeader = false
	}
	return index, term, isLeader
}

func (rf *Raft) applyToLocalServiceReplica(currentIndex int) bool {
	applySucc := false
	if currentIndex >= len(rf.logs) {
		return applySucc
	}
	msg := ApplyMsg{}
	msg.Index = currentIndex
	msg.Command = rf.logs[currentIndex].Command
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentIndex = rf.lastApplied + 1
	fmt.Printf("Peer-%d check the index before apply. Want=%d, real=%d.\n", rf.me, msg.Index, currentIndex)
	if currentIndex == msg.Index {
		rf.applyChan <- msg
		rf.lastApplied = currentIndex
		applySucc = true
	}
	if applySucc {
		fmt.Printf("peer-%d has send applyMsg={%d, %v} to channel.\n", rf.me, msg.Index, msg.Command)
	} else {
		fmt.Printf("current applied index is changed. expect=%d, actual=%d\n", currentIndex, msg.Index)
	}
	return applySucc
}

func (rf *Raft) createAppendEntryRequest(currentIndex int, currentTerm int, heartBeat bool) *AppendEntryArgs {
	currentLen := len(rf.logs)
	if currentIndex <= 0 || currentIndex > currentLen {
		fmt.Printf("Peer-%d receive an illegal currentIndex=%d, largest log index=%d\n", rf.me, currentIndex, len(rf.logs))
		return nil
	}
	request := new(AppendEntryArgs)
	request.Term = currentTerm
	request.LeaderId = rf.me
	request.LeaderCommit = rf.commitIndex
	prevLogIndex := currentIndex - 1
	if prevLogIndex >= 0 {
		request.PrevLogIndex = prevLogIndex
		request.PrevLogTerm = rf.logs[prevLogIndex].Term
	} else {
		request.PrevLogIndex = 0
		request.PrevLogTerm = 0
	}
	if !heartBeat {
		request.Entry = rf.logs[currentIndex]
	}
	fmt.Printf("Peer-%d create a request: {hb=%t, cidx=%d, term=%d, cmt=%d, pidx=%d, pterm=%d, v=%v}\n", rf.me, heartBeat, currentIndex, request.Term, request.LeaderCommit, request.PrevLogIndex, request.PrevLogTerm, request.Entry)
	return request
}

func (rf *Raft) appendToServers(currentIndex int, currentTerm int) bool {
	fmt.Printf("Peer-%d begin to appendToServers.\n", rf.me)
	doneCh := make(chan int)
	doneCount := 0
	hasCommited := false
	request := rf.createAppendEntryRequest(currentIndex, currentTerm, false)
	for i := 0; rf.isLeader && i < len(rf.peers); i++ {
		if i == rf.me {
			doneCount++
			continue
		}
		go func(server int) {
			// make sure nextIndex of the server has matched current commitIndex
			fmt.Printf("Peer-%d begin to append log to peer-%d with index-%d.\n", rf.me, server, currentIndex)
			success := rf.appendToServerWithCheck(server, currentIndex, request)
			if success {
				fmt.Printf("Peer-%d has append log to peer-%d successfully, begin to write doneCh.\n", rf.me, server)
				doneCh <- server
				fmt.Printf("Peer-%d has append log to peer-%d successfully, writing doneCh is over.\n", rf.me, server)
			} else {
				fmt.Printf("Peer-%d append log to peer-%d with index %d failed.\n", rf.me, server, currentIndex)
			}
			/*
				nextLogIndex := currentIndex
				for !rf.isStopping {
					//rf.serverMu.RLock()
					if rf.nextIndex[server] > currentIndex {
						break
					}
					fmt.Printf("Peer-%d try to send append to peer-%d, nextIndex=%d, currentIndex=%d\n", rf.me, server, rf.nextIndex[server], currentIndex)
					//rf.serverMu.RUnlock()
					if request == nil {
						// log it.
						fmt.Printf("Peer-%d has received a null request.\n", server)
						break
					}
					if !rf.isLeader {
						fmt.Printf("Peer-%d is not leader, stop to append entries to peer-%d.\n", rf.me, server)
						break
					}
					isSuccess := rf.appendToServer(server, nextLogIndex, request)
					//rf.serverMu.RLock()
					if isSuccess {
						if rf.nextIndex[server] == currentIndex+1 {
							fmt.Printf("Peer-%d has append log to peer-%d successfully, begin to write doneCh.\n", rf.me, server)
							doneCh <- server
							fmt.Printf("Peer-%d has append log to peer-%d successfully, writing doneCh is over.\n", rf.me, server)
						}
					} else {
						rf.mu.Lock()
						nextLogIndex = rf.nextIndex[server]
						if nextLogIndex <= 1 {
							nextLogIndex = 1
						}
						rf.nextIndex[server] = nextLogIndex - 1
						currentTerm = rf.currentTerm
						rf.mu.Unlock()
						if nextLogIndex > currentIndex {
							break
						}
						fmt.Printf("Peer-%d append log to peer-%d failed, try nextIndex=%d\n", rf.me, server, nextLogIndex)
						request = rf.createAppendEntryRequest(nextLogIndex, currentTerm, false)
					}
					//rf.serverMu.RUnlock()
					rf.sleep(10)
				}
			*/
		}(i)
	}
	if rf.isStopping {
		// false?
		return false
	}
	stopRunning := false
	for !stopRunning {
		select {
		case server := <-doneCh:
			if server >= 0 && server < len(rf.peers) {
				fmt.Printf("Peer-%d confirm peer-%d has done.\n", rf.me, server)
				doneCount++
				if doneCount >= len(rf.peers)/2+1 {
					fmt.Printf("Peer-%d confirm majority.\n", rf.me)
					hasCommited = true
					stopRunning = true
				}
			} else {
				// log it.
				fmt.Printf("Leader-%d receieve a error server index=%d\n", rf.me, server)
			}
		case <-time.After(time.Duration(1000) * time.Millisecond):
			fmt.Printf("Peer-%d agreement is timeout.\n", rf.me)
			stopRunning = true
		}
	}
	fmt.Printf("Peer-%d channel select is done.\n", rf.me)
	if hasCommited {
		fmt.Printf("Peer-%d has commit currentIndex=%d\n", rf.me, currentIndex)
		rf.mu.Lock()
		if currentIndex >= rf.commitIndex {
			rf.commitIndex = currentIndex
		}
		rf.mu.Unlock()
	}
	return hasCommited
}

func (rf *Raft) appendToServerWithCheck(server int, currentIndex int, request *AppendEntryArgs) bool {
	nextLogIndex := currentIndex
	for !rf.isStopping {
		//rf.serverMu.RLock()
		if rf.nextIndex[server] > currentIndex {
			break
		}
		fmt.Printf("Peer-%d try to send append to peer-%d, nextIndex=%d, currentIndex=%d\n", rf.me, server, rf.nextIndex[server], currentIndex)
		//rf.serverMu.RUnlock()
		if request == nil {
			// log it.
			fmt.Printf("Peer-%d has received a null request.\n", server)
			break
		}
		if !rf.isLeader {
			fmt.Printf("Peer-%d is not leader, stop to append entries to peer-%d.\n", rf.me, server)
			break
		}

		isHeartbeat := request.Entry.Command == nil
		isSuccess := rf.appendToServer(server, nextLogIndex, request)
		//rf.serverMu.RLock()
		if isSuccess {
			if rf.nextIndex[server] == currentIndex+1 {
				return true
			}
		} else {
			rf.mu.Lock()
			nextLogIndex = rf.nextIndex[server] - 1
			if nextLogIndex >= 1 {
				rf.nextIndex[server] = nextLogIndex
			}
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if nextLogIndex == 0 || nextLogIndex > currentIndex {
				break
			}
			fmt.Printf("Peer-%d append log to peer-%d failed, try nextIndex=%d\n", rf.me, server, nextLogIndex)
			request = rf.createAppendEntryRequest(nextLogIndex, currentTerm, false)
		}
		//rf.serverMu.RUnlock()
		rf.sleep(10)
	}
	return false
}

func (rf *Raft) appendToServer(server int, currentIndex int, request *AppendEntryArgs) bool {
	if currentIndex >= rf.nextIndex[server] {
		ok := false
		var reply *AppendEntryReply
		var retryCount = 0
		for !ok && rf.isLeader && retryCount < 10 {
			reply = new(AppendEntryReply)
			ok = rf.sendAppendEntry(server, request, reply)
			rf.sleep(100 * retryCount)
			retryCount++
		}
		fmt.Printf("Peer-%d has sent request to peer-%d\n", rf.me, server)
		appendSucc := reply != nil && reply.Success
		isHeartbeat := request.Entry.Command == nil
		//rf.serverMu.RLock()
		if reply != nil && reply.Success && !isHeartbeat {
			rf.mu.Lock()
			if currentIndex >= rf.nextIndex[server] {
				rf.nextIndex[server] = currentIndex + 1
				fmt.Printf("Peer-%d has set nextIndex[%d] to %d\n", rf.me, server, rf.nextIndex[server])
			}
			if currentIndex >= rf.matchIndex[server] {
				rf.matchIndex[server] = currentIndex
				fmt.Printf("Peer-%d has set matchIndex[%d] to %d\n", rf.me, server, rf.matchIndex[server])
			}
			rf.mu.Unlock()
		} else if reply != nil {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				fmt.Printf("Peer-%d received reply from peer-%d, it's term=%d is larger than currentTerm=%d\n", rf.me, server, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.isLeader = false
			}
			rf.mu.Unlock()
		}
		//rf.serverMu.RUnlock()
		fmt.Printf("Peer-%d has received reply from peer-%d, %t\n", rf.me, server, appendSucc)
		return reply != nil && reply.Success
	} else {
		return false
	}
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
	fmt.Printf("Call Kill() of peer-%d, try to stop the raft.\n", rf.me)
	rf.serverMu.Lock()
	defer rf.serverMu.Unlock()
	rf.isStopping = true
	fmt.Printf("Call Kill() of peer-%d successfully.\n", rf.me)
}

func (rf *Raft) startApplyService() {
	go func() {
		fmt.Printf("peer-%d start thread to send applyMsg!\n", rf.me)
		currentIndex := 0
		for !rf.isStopping {
			//rf.serverMu.RLock()
			canApply := false
			rf.mu.Lock()
			currentIndex = rf.lastApplied + 1
			if currentIndex != 0 && currentIndex <= rf.commitIndex {
				canApply = true
			}
			logsLength := len(rf.logs)
			rf.mu.Unlock()
			//rf.serverMu.RUnlock()
			if !canApply {
				rf.sleep(400)
				continue
			}
			fmt.Printf("Peer-%d try to apply message: currentIndex=%d.\n", rf.me, currentIndex)
			if currentIndex > logsLength {
				fmt.Printf("peer-%d's currentIndex(%d) is larger than rf.logs' length(%d)\n", rf.me, currentIndex, logsLength)
				break
			}
			applySucc := rf.applyToLocalServiceReplica(currentIndex)
			if !applySucc {
				rf.sleep(400)
			}
		}
	}()
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
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	atomic.StoreInt64(&rf.lastTick, time.Now().UnixNano())

	// init nextIndex and matchIndex
	logSize := len(rf.logs)
	for key := 0; key < len(rf.peers); key++ {
		rf.nextIndex[key] = logSize
		rf.matchIndex[key] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.startApplyService()

	go func() {
		for !rf.isStopping {
			if rf.isLeader {
				// send heartbeat
				/*
						var req = new(AppendEntryArgs)
						req.Term = rf.currentTerm
						req.LeaderCommit = rf.commitIndex
					for i := 0; i < len(rf.peers); i++ {
						if !rf.isLeader {
							// check isLeader every time.
							break
						}
						if i == rf.me {
							continue
						}
						server := i
						go func() {
							fmt.Printf("leader-%d send heartbeat to peer-%d\n", rf.me, server)
							rep := new(AppendEntryReply)
							ok := rf.sendAppendEntry(server, req, rep)
							if !ok {
								// retry?
							} else {
								fmt.Printf("leader-%d has sent heartbeat to peer-%d\n", rf.me, server)
								rf.mu.Lock()
								if rep != nil && rf.currentTerm < rep.Term {
									fmt.Printf("leader-%d's term %d is smaller than peer-%d's term %d. Turn to follower\n", rf.me, rf.currentTerm, server, rep.Term)
									atomic.StoreInt64(&rf.lastTick, time.Now().UnixNano())
									rf.currentTerm = rep.Term
									rf.isLeader = false
								}
								rf.mu.Unlock()
							}
						}()
					}
				*/
				fmt.Printf("Peer-%d begin to send heartbeat.\n", rf.me)
				currentIndex := rf.commitIndex + 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if !rf.isLeader {
						break
					}
					go func(server int) {
						request := rf.createAppendEntryRequest(currentIndex, rf.currentTerm, true)
						rf.appendToServerWithCheck(server, currentIndex, request)
					}(i)
				}
				time.Sleep(499 * time.Millisecond)
			} else {
				sleepDuration := 1000 - int(time.Now().UnixNano()-atomic.LoadInt64(&rf.lastTick))/1000000
				fmt.Printf("peer-%d want to sleep %d\n", rf.me, sleepDuration)
				if sleepDuration <= 0 {
					rf.mu.Lock()
					rf.isVoting = true
					rf.currentTerm += 1
					reqTerm := rf.currentTerm
					logIndex := len(rf.logs) - 1
					rf.mu.Unlock()
					var logTerm = 0
					if logIndex >= 0 {
						fmt.Printf("peer-%d get logIndex=%d\n", rf.me, logIndex)
						logTerm = rf.logs[logIndex].Term
					}
					var voteForMe int32 = 0
					var waitVote sync.WaitGroup
					for i := 0; i < len(peers); i++ {
						waitVote.Add(1)
						fmt.Printf("peer-%d wants to send vote request to peer-%d\n", rf.me, i)
						if i == rf.me {
							// put the judge logic to a function
							atomic.AddInt32(&voteForMe, 1)
							waitVote.Done()
							continue
						}
						server := i
						go func() {
							fmt.Printf("peer-%d sends vote request to peer-%d\n", rf.me, server)
							var req RequestVoteArgs
							req.Term = reqTerm
							req.Candidate = rf.me
							req.LastLogIndex = logIndex
							req.LastLogTerm = logTerm
							fmt.Printf("peer-%d begin to vote. request=(%d, %d, %d)\n", req.Candidate, req.Term, req.LastLogIndex, req.LastLogTerm)
							var rep = new(RequestVoteReply)
							// rep.term = -1
							// rep.voteGrant = false
							ok := rf.sendRequestVote(server, &req, rep)
							if ok {
								fmt.Printf("peer-%d received vote response from peer-%d.\n", req.Candidate, server)
								if rep != nil && rep.VoteGrant {
									fmt.Printf("in peer-%d, peer-%d grant, count=%d.\n", rf.me, server, voteForMe)
									atomic.AddInt32(&voteForMe, 1)
								} else if rep != nil {
									if rep.Term > rf.currentTerm {
										fmt.Printf("peer-%d update term from %d to %d\n", rf.currentTerm, rep.Term)
										rf.mu.Lock()
										rf.currentTerm = rep.Term
										rf.mu.Unlock()
									}
								} else {
									fmt.Printf("reply from peer-%d is null.\n", server)
								}
							} else {
								// considering retry.
								fmt.Printf("peer-%d received null vote response from peer-%d.\n", req.Candidate, server)
							}
							waitVote.Done()
						}()
					}
					doneChan := make(chan struct{})
					go func() {
						defer close(doneChan)
						waitVote.Wait()
					}()
					var isTimeout = false
					select {
					case <-doneChan:
						isTimeout = false
					case <-time.After(time.Duration(1000) * time.Millisecond):
						isTimeout = true
					}
					if voteForMe >= int32(len(peers)/2+1) {
						fmt.Printf("peer-%d win, vote granted.\n", rf.me)
						rf.mu.Lock()
						rf.isLeader = true
						rf.mu.Unlock()
					} else {
						fmt.Printf("peer-%d vote failed, timeout=%t\n", rf.me, isTimeout)
						rf.sleep(500)
					}
					rf.mu.Lock()
					rf.isVoting = false
					rf.mu.Unlock()
				} else {
					rf.isVoting = false
					realSleepTime := sleepDuration + rand.Intn(100)
					b := time.Now()
					time.Sleep(time.Duration(realSleepTime) * time.Millisecond)
					a := time.Now()
					fmt.Printf("duration: %v\n", a.Sub(b))
					fmt.Printf("peer-%d has sleep %d.\n", rf.me, realSleepTime)
				}
			}
		}
	}()

	return rf
}
