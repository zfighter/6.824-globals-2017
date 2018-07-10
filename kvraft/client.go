package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type RetryState int

const (
	DontRetry RetryState = iota
	RetryWithNewIndex
	RetryWithOldIndex
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverCount  int32
	maxRetryTime int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.serverCount = len(servers)
	return ck
}

func (ck *Clerk) getServer(server int32) (*labrpc.ClientEnd, serverIndex) {
	randIndex := server
	if server != -1 {
		randNumber := nrand() % ck.serverCount
		randIndex = int32(randNumber)
	}
	return servers[randIndex], randIndex
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	result := ""
	if key != nil {
		request := GetArgs{}
		request.Key = key
		request.Nonce = nrand()
		// result = ck.GetInternal(&request, 1, -1)
		needRetry := RetryWithNewIndex
		result := ""
		serverIndex := -1
		for needRetry != DontRetry {
			if needRetry == RetryWithNewIndex {
				serverIndex = ck.getServer(serverIndex)
			}
			result, needRetry = ck.GetInternal(&request, serverIndex)
		}
	}
	return result
}

func (ck *Clerk) GetInternal(getRequest *GetArgs, serverIndex int32) (string, RetryState) {
	server := ck.servers[serverIndex]
	getReply := GetReply{}
	ok := server.Call("RaftKV.Get", getRequest, &getReply)
	result := ""
	retry := DontRetry
	if ok {
		if getReply.WrongLeader {
			retry = RetryWithNewIndex
		} else {
			if getReply.Err == OK || getReply.Err == ErrNoKey {
				result = getReply.Value
			} else {
				retry = RetryWithOldIndex
			}
		}
	} else {
		retry = RetryWithNewIndex
	}
	return result, retry
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if key != nil {
		request := PutAppendArgs{}
		request.Key = key
		request.Value = value
		request.Op = op
		request.Nonce = nrand()
		needRetry := RetryWithNewIndex
		serverIndex := -1
		for needRetry {
			// TODO: do real put.
			if needRetry == RetryWithNexIndex {
				serverIndex = ck.getServer(serverIndex)
			}
			PutInternal(&request, serverIndex)
		}
	}
}

func (ck *Clerk) PutInternal(putAppendArgs *PutAppendArgs, serverIndex int32) RetryState {
	server := ck.servers[serverIndex]
	putAppendReply := PutAppendReply{}
	ok := server.Call("RaftKV.PutAppend", putAppendArgs, &putAppendReply)
	retry := DontRetry
	if ok {
		if putAppendReply.WrongLeader {
			retry = RetryWithNewIndex
		} else {
			if putAppendReply.Err != OK {
				retry = RetryWithOldIndex
			}
		}
	} else {
		retry = RetryWithNewIndex
	}
	return retry
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
