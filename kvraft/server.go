package raftkv

import (
	"encoding/gob"
	"github.com/6.824/labrpc"
	"github.com/6.824/raft"
	"log"
	"sync"
	"time"
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
	Nonce  int64
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
	appliedIndex int // the index of applied message

	// Your definitions here.
	kvStore    map[string]string // key -> value
	nonceCache map[int64]int64   // nonce -> time

	stopping bool // stop singal.
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// start nonce
	doNonce := false
	if args.Nonce > 0 {
		doNonce := kv.startNonce(args.Nonce)
		if !doNonce {
			reply.Err = "Duplicated request"
			reply.WrongLeader = false
			return
		}
	}
	// create Op
	op := Op{}
	op.OpType = "Get"
	op.Key = args.Key
	op.Nonce = args.Nonce
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		if doNonce {
			kv.deleteNonce(args.Nonce)
		}
	} else {
		startTime := time.Now()
		reply.WrongLeader = false
		for time.Since(startTime).Seconds() < electionTimeout.Seconds() {
			if kv.appliedIndex < index {
				time.Sleep(10 * time.Millisecond)
				reply.Err = "Timeout"
			} else {
				value, found := kv.kvStore[args.Key]
				if found {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
				}
			}
		}
		if reply.Err != OK && reply.Err != ErrNoKey {
			if doNonce {
				kv.deleteNonce(args.Nonce)
			}
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// start nonce
	doNonce := false
	if args.Nonce > 0 {
		doNonce := kv.startNonce(args.Nonce)
		if !doNonce {
			reply.Err = "Duplicated request"
			reply.WrongLeader = false
			return
		}
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
		if doNonce {
			kv.deleteNonce(args.Nonce)
		}
	} else {
		// keep checking before timeout.
		startTime := time.Now()
		for time.Since(startTime).Seconds() < electionTimeout.Seconds() {
			if kv.rf.commitIndex < index {
				time.Sleep(10 * time.Millisecond)
				reply.WrongLeader = false
				reply.Err = "Timeout"
			} else {
				reply.WrongLeader = false
				reply.Err = OK
			}
		}
		if reply.Err != OK {
			if doNonce {
				kv.deleteNonce(args.Nonce)
			}
		}
	}
}

func (kv *RaftKV) startNonce(key int64) (success bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	success = false
	// check nonce
	nonceTimestamp, found := kv.nonceCache[key]
	if !found {
		return
	}
	// add nonce to cache
	kv.nonceCache[key] = time.Now().Unix()
	success = true
	return
}

func (kv *RaftKV) deleteNonce(key int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.nonceCache, key)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.rf.Kill()
	// Your code here, if desired.
	kv.stopping = true
}

// ====== service ======
func (kv *RaftKV) applyService() {
	// TODO: monitor applyCh for new ApplyMsg; but it should try to stop when receive signal from StopCh
	for applyMsg := range kv.applyCh {
		kv.appliedIndex = applyMsg.Index
		// TODO: should check nonce in store operations?
		op := (applyMsg.Command).(Op)
		switch opType := op.OpType; opType {
		case "Put":
			kv.kvStore[op.Key] = op.Value
		case "Append":
			currValue, has := kv.kvStore[op.Key]
			if !has {
				kv.kvStore[op.Key] = op.Value
			} else {
				newValue := currValue + op.Value
				kv.kvStore[op.Key] = newValue
			}
		case "Get":
			currValue, has := kv.kvStore[op.Key]
			op.GetRep.WrongLeader = false
			if !has {
				op.GetRep.Value = ""
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

func (kv *RaftKV) nonceRefreshService() {
	// refresh nonce cache every ten minutes.
	for !kv.stopping {
		nonceCacheSize := len(kv.nonceCache)
		if nonceCacheSize >= 1024*1024 {
			currTime := time.Now().Unix()
			checkTime := currTime - 600
			// TODO: is this operation thread-safe?
			for key, value := range kv.nonceCache {
				// to check timeout and clear the key-values timeouted.
				if value <= checkTime {
					kv.deleteNonce(key)
				}
			}
		}
		time.Sleep(time.Duration(600) * time.Second)
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
	kv.nonceCache = make(map[int64]int64)
	kv.stopping = false
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// start applyService
	go kv.applyService()
	// start nonceRefreshService
	go kv.nonceRefreshService()

	return kv
}
