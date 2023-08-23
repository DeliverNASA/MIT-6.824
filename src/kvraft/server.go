package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

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
	CR     ClientRequest
	Key    string
	Value  string
	Method string
}

type Message struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data       map[string]string
	MessageCh  map[ClientRequest]chan Message
	RequestMap map[ClientRequest]bool
}

// get (key, value) from KVS, return ErrNoKey if key doesn't exist
func (kv *KVServer) dataGet(key string) (Err, string) {
	value, exists := kv.data[key]
	if exists {
		return OK, value
	} else {
		return ErrNoKey, ""
	}
}

// check if the request has been handled
func (kv *KVServer) isRepeated(CR ClientRequest) bool {
	_, exists := kv.RequestMap[CR]
	return exists
}

// check if the request is being handled
func (kv *KVServer) isBeingHandled(CR ClientRequest) bool {
	_, exists := kv.MessageCh[CR]
	return exists
}

// start a timer to avoid request being blocked
func (kv *KVServer) timeout(CR ClientRequest) {
	time.Sleep(REPLYTIMEOUT)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.isBeingHandled(CR) {
		DPrintf("[%v] REQ %v timeout", kv.me, CR)
		msg := Message{Err: ErrTimeOut}
		DPrintf("[%v] try to input msgCh %v ...", kv.me, CR)
		kv.MessageCh[CR] <- msg
		DPrintf("[%v] success to input msgCh %v", kv.me, CR)
		delete(kv.MessageCh, CR)
		DPrintf("[%v] delete messageCh %v", kv.me, CR)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// if not leader, reject the get request
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// DPrintf("%v is not a leader, cannot handle request %v", kv.me, args.RequestId)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] is a leader, now handle REQ %v", kv.me, args.CR)

	kv.mu.Lock()
	isBeingHandled := kv.isBeingHandled(args.CR)
	kv.mu.Unlock()

	// avoid handling the same request concurrently
	if isBeingHandled {
		time.Sleep(BeingHandledInterval)
		reply.Err = ErrTimeOut
		return
	}

	// start a agreement
	op := Op{
		CR:     args.CR,
		Key:    args.Key,
		Method: GET,
	}
	res := kv.waitcmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
	DPrintf("[%v] finish REQ %v %v receive (key, value) = (%v, %v)", kv.me, args.CR, op.Method, op.Key, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		// DPrintf("%v is not a leader, cannot handle request %v", kv.me, args.RequestId)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] is a leader, now handle REQ %v", kv.me, args.CR)

	kv.mu.Lock()
	isRepeated := kv.isRepeated(args.CR)
	isBeingHandled := kv.isBeingHandled(args.CR)
	kv.mu.Unlock()

	// avoid repeated request
	if isRepeated {
		DPrintf("[%v] REQ %v has been handled", kv.me, args.CR)
		reply.Err = OK
		return
	}

	// avoid handling the same request concurrently
	if isBeingHandled {
		time.Sleep(BeingHandledInterval)
		reply.Err = ErrTimeOut
		return
	}

	op := Op{
		CR:     args.CR,
		Key:    args.Key,
		Value:  args.Value,
		Method: args.Op,
	}
	res := kv.waitcmd(op)
	reply.Err = res.Err
	DPrintf("[%v] finish REQ %v %v (key, value) = (%v, %v)", kv.me, args.CR, op.Method, op.Key, op.Value)
}

// wait util command executed or timeout
func (kv *KVServer) waitcmd(op Op) (res Message) {
	DPrintf("[%v] try to get the lock in WAITCMD", kv.me)
	kv.mu.Lock()
	DPrintf("[%v] success to get the lock in WAITCMD", kv.me)
	ch := make(chan Message)
	kv.MessageCh[op.CR] = ch
	DPrintf("[%v] make messageCh %v", kv.me, op.CR)
	DPrintf("[%v] release the lock in WAITCMD", kv.me)
	kv.mu.Unlock()

	// start an agreement
	kv.rf.Start(op)

	// start a timeout goroutine
	go kv.timeout(op.CR)

	select {
	// only receive message (TIMEOUT or NORMAL) can go on
	case msg := <-ch:
		res.Err = msg.Err
		if msg.Err != ErrTimeOut {
			DPrintf("[%v] receive REQ %v agreement Message, err = %v, value = %v", kv.me, op.CR, msg.Err, msg.Value)
			res.Value = msg.Value
		}
	}
	return res
}

// handle commands from ApplyCh
func (kv *KVServer) HandleApplyCh() {
	for {
		select {
		case command := <-kv.applyCh:
			DPrintf("[%v] waiting ApplyCh...", kv.me)
			// DPrintf("%v get command %v, valid = %v, index = %v", kv.me, command.Command.(Op), command.CommandValid, command.CommandIndex)

			if !command.CommandValid {
				DPrintf("command is invalid")
				continue
			}
			kv.mu.Lock()
			if op, ok := command.Command.(Op); ok {
				switch op.Method {
				case PUT:
					if !kv.isRepeated(op.CR) {
						kv.data[op.Key] = op.Value
						DPrintf("[%v] REQ %v, PUT OK. (key, value) = (%v, %v)", kv.me, op.CR, op.Key, kv.data[op.Key])
					}
				case APPEND:
					_, value := kv.dataGet(op.Key)
					if !kv.isRepeated(op.CR) {
						kv.data[op.Key] = value + op.Value
						DPrintf("[%v] REQ %v, APPEND OK. (key, value) = (%v, %v)", kv.me, op.CR, op.Key, kv.data[op.Key])
					}
				case GET:
				default:
					DPrintf("cannot recoginize this data type %v", op.Method)
				}

				// only guarantee PUT or APPEND exactly-once
				if op.Method == PUT || op.Method == APPEND {
					kv.RequestMap[op.CR] = true
				}

				// only leader needs to reply to the client
				if kv.isBeingHandled(op.CR) {
					err, value := kv.dataGet(op.Key)
					msg := Message{Err: err, Value: value}
					DPrintf("[%v] try to input msgCh %v ...", kv.me, op.CR)
					kv.MessageCh[op.CR] <- msg
					DPrintf("[%v] success to input msgCh %v", kv.me, op.CR)
					delete(kv.MessageCh, op.CR)
					DPrintf("[%v] delete messageCh %v", kv.me, op.CR)
				}
			} else {
				DPrintf("cannot restore as a Op type.")
			}
			kv.mu.Unlock()
		}
		DPrintf("[%v] finish ApplyCh", kv.me)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.MessageCh = make(map[ClientRequest]chan Message)
	kv.RequestMap = make(map[ClientRequest]bool)

	// You may need initialization code here.
	go kv.HandleApplyCh()

	return kv
}
