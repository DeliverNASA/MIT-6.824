package kvraft

import "time"

const (
	OK                   = "OK"
	ErrNoKey             = "ErrNoKey"
	ErrWrongLeader       = "ErrWrongLeader"
	ErrTimeOut           = "ErrTimeOut"
	GET                  = "Get"
	PUT                  = "Put"
	APPEND               = "Append"
	REPLYTIMEOUT         = time.Second * 2
	ChangeLeaderInterval = time.Millisecond * 100
	BeingHandledInterval = time.Millisecond * 500
)

type Err string

type ClientRequest struct {
	ClientId  int
	RequestId int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	CR    ClientRequest
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	CR  ClientRequest
}

type GetReply struct {
	Err   Err
	Value string
}
