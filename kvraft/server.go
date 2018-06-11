package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Nonce  int32
	Key    string
	Value  string    // for put/append
	GetRep *GetReply // for get
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore    map[string]string // key -> value
	nonceCache map[int32]int32   // nonce -> time

	stopping bool // stop singal.
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check nonce
	nonceTimestamp := nonceCache[args.Nonce]
	if nonceTimestamp != nil {
		reply.Err = "Duplicated request"
		reply.WrongLeader = false
		return
	}
	// create Op
	op := Op{}
	op.OpType = args.Op
	op.Key = args.Key
	op.Nonce = args.Nonce
	op.Value = args.Value
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	} else {
		// keep checking before timeout.
		startTime := time.Now()
		for time.Since(startTime).Seconds() < electionTimeout {
			if kv.rf.commitIndex < index {
				time.Sleep(10 * time.Millisecond)
				reply.WrongLeader = false
				reply.Err = "Timeout"
			} else {
				reply.WrongLeader = false
				reply.Err = OK
				nonceCache[args.Nonce] = time.Now()
			}
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.stopping = true
}

// ====== service ======
func (kv *RaftKV) applyService() {
	// TODO: monitor applyCh for new ApplyMsg; but it should try to stop when receive signal from StopCh
	for op := range kv.applyCh {
		// TODO: should check nonce in store operations?
		switch opType := op.OpType; opType {
		case "Put":
			kv.kvStore[op.Key] = op.Value
		case "Append":
			currValue := kv.kvStore[op.Key]
			if currValue == nil {
				kv.kvStore[op.Key] = op.Value
			} else {
				newValue := currValue + op.Value
				kv.kvStore[op.Key] = newValue
			}
		case "Get":
			currValue := kv.kvStore[op.Key]
			op.GetRep.WrongLeader = false
			if currValue == nil {
				op.GetRep.Value = nil
				op.GetRep.Err = ErrNoKey
			} else {
				op.GetRep.Value = currValue
				op.GetRep.Err = OK
			}
		default:
			DPrintf("Don't support op type: %s", opType)
		}
	}
}

func (kv *RaftKV) nonceFlashService() {
	// TODO: flash nonce cache every ten minutes.
	for !kv.stopping {
		nonceCacheSize := size(nonceCache)
		if nonceCacheSize > 1024*1024 {
			kv.mu.Lock()
			currTime := time.Now()
			for key, value := range nonceCache {
				// TODO: use time to check timeout and clear the key-values timeouted.

			}
			kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.stopping = false
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// start applyService
	go kv.applyService()

	return kv
}
