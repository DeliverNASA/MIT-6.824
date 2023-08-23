package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	serverNum int
	leaderId  int
	clientId  int
	commandId int
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
	ck.serverNum = len(servers)
	ck.leaderId = 0
	ck.clientId = int(nrand())
	ck.commandId = 0
	return ck
}

func (ck *Clerk) GetCommandId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	id := ck.commandId
	ck.commandId++
	return id
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// return ck.sendRequest(GET, key, "")

	requestId := ck.GetCommandId()
	args := GetArgs{
		Key: key,
		CR: ClientRequest{
			ClientId:  ck.clientId,
			RequestId: requestId,
		},
	}
	reply := GetReply{}
	leaderId := ck.leaderId

	for {
		DPrintf("CLERK [%v]: REQ %v send call to %v, %v key = %v, value = %v", ck.clientId, requestId, leaderId, GET, key, "")
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if !ok {
			DPrintf("CLERK [%v]: RPC %v no response", ck.clientId, requestId)
			time.Sleep(ChangeLeaderInterval)
			// DPrintf("leader id: %v -> %v", leaderId, (leaderId+1)%ck.serverNum)
			leaderId = (leaderId + 1) % ck.serverNum
			continue
		}

		DPrintf("CLERK [%v]: REQ %v receives %v reply.Err %v", ck.clientId, requestId, leaderId, reply.Err)

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			DPrintf("CLERK [%v]: record leader: %v", ck.clientId, ck.leaderId)
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			return ""
		case ErrTimeOut:
			ck.leaderId = leaderId
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % ck.serverNum
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// ck.sendRequest(op, key, value)
	requestId := ck.GetCommandId()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CR: ClientRequest{
			ClientId:  ck.clientId,
			RequestId: requestId,
		},
	}
	reply := PutAppendReply{}
	leaderId := ck.leaderId

	for {
		DPrintf("CLERK [%v]: REQ %v send call to %v, %v key = %v, value = %v", ck.clientId, requestId, leaderId, op, key, value)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			DPrintf("CLERK [%v]: RPC %v no response", ck.clientId, requestId)
			time.Sleep(ChangeLeaderInterval)
			// DPrintf("leader id: %v -> %v", leaderId, (leaderId+1)%ck.serverNum)
			leaderId = (leaderId + 1) % ck.serverNum
			continue
		}

		DPrintf("CLERK [%v]: REQ %v receives %v reply.Err %v", ck.clientId, requestId, leaderId, reply.Err)

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			DPrintf("CLERK [%v]: record leader: %v", ck.clientId, ck.leaderId)
			return
		case ErrTimeOut:
			ck.leaderId = leaderId
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % ck.serverNum
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
