package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	raftLeader int
	SequenceId int
	clientId int64
	mu sync.Mutex
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
	ck.raftLeader = 0
	ck.SequenceId = 0
	ck.clientId = nrand()
	DPrintf("MakeClerk : Clerk %d init",ck.clientId)
	time.Sleep(time.Duration(700)*time.Millisecond)
	return ck
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

	// You will have to modify this function.
	ck.mu.Lock()
	ck.SequenceId++
	DPrintf("Client-Get:cmd seq = %d and client = %d\n",ck.SequenceId,ck.clientId)
	args := GetArgs{
		Key: key,
		RequestTag: Tag{
			ClientId: ck.clientId,
			SeqId: ck.SequenceId,
		},
	}
	reply := GetReply{
		Err: "",
	}
	server := ck.raftLeader
	ck.mu.Unlock()
	ok := false
	for !ok || reply.Err != OK{
		reply.Err = OK
		DPrintf("Client-Get:cmd has been send to kv %d\n",server)
		ok = ck.servers[server].Call("KVServer.Get",&args,&reply)
		if ok{
			if reply.Err == OK{
				return reply.Value
			}
			if reply.Err == ErrNoKey{
				return ""
			}
		}
		DPrintf("Client-Get:server = %d ok = %v reply = %v\n",server,ok,reply)
		server = (server+1)%len(ck.servers)
		ck.mu.Lock()
		ck.SequenceId++
		args.RequestTag.SeqId = ck.SequenceId
		ck.raftLeader = server
		ck.mu.Unlock()

	}
	return reply.Value
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
	ck.mu.Lock()
	ck.SequenceId++
	DPrintf("Client-PutAppend:cmd seq = %d\n",ck.SequenceId)
	args := PutAppendArgs{
		Key: key,
		Operation: op,
		Value: value,
		RequestTag: Tag{
			ClientId: ck.clientId,
			SeqId: ck.SequenceId,
		},
	}
	reply := GetReply{
		Err: "",
	}
	server := ck.raftLeader
	ck.mu.Unlock()
	ok := false
	for !ok || reply.Err != OK{
		reply.Err = OK
		DPrintf("Client-PutAppend:%v cmd has been send to kv %d\n",args.Operation,server)
		ok = ck.servers[server].Call("KVServer.PutAppend",&args,&reply)
		if reply.Err == OK && ok{
			return
		}
		DPrintf("Client-PutAppend:server = %d ok = %v reply = %v\n",server,ok,reply)
		server = (server+1)%len(ck.servers)
		ck.mu.Lock()
		ck.raftLeader = server
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
