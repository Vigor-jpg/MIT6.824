package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)


type Tag struct {
	ClientId int64
	SeqId int
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator string
	Key string
	Value string
	RequestTag Tag
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	
	// Your definitions here.
	lastApplied int
	opIndex int
	kvMap map[string]string
	kvOp map[Tag]chan string
	isDuplicate map[Tag] int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	DPrintf("Get : kv %d has been rec a Get",kv.me)
	//if count,ok := kv.isDuplicate[args.RequestTag];ok{
	//	if count ==
	//}else{
	//
	//}
	op := Op{
		Operator: "Get",
		Key: args.Key,
		RequestTag: args.RequestTag,
	}
	//kv.isDuplicate[args.RequestTag] = 0
	//ch := make(chan string,1)
	//kv.kvOp[args.RequestTag] = ch
	_,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		//close(ch)
		return
	}
	ch,ok := kv.kvOp[args.RequestTag]
	if !ok{
		ch = make(chan string,1)
		kv.kvOp[args.RequestTag] = ch
	}
	DPrintf("Get : kv %d Tag = %v\n",kv.me,args.RequestTag)
	kv.mu.Unlock()
	t := time.Now()
	select {
	case res := <- ch:
		reply.Value = res
		reply.Err = OK
		close(ch)
		DPrintf("Get : kv %d has read the channel",kv.me)
	case <- time.After(2000*time.Millisecond):
		reply.Err = ErrTimeOut
		DPrintf("kv Get= %d time = %v",kv.me,time.Since(t))
		//return
	}
	DPrintf("Get : kv %d has return",kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//kv.mu.Lock()
	op := Op{
		Operator: args.Operation,
		Key: args.Key,
		Value: args.Value,
		RequestTag: args.RequestTag,
	}
	DPrintf("PutAppend : kv %d has been rec a putAppend and op = %v",kv.me,op)
	//kv.isDuplicate[args.RequestTag] = 0
	//ch := make(chan string,1)
	//kv.kvOp[args.RequestTag] = ch
	_,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		//kv.mu.Unlock()
		//close(ch)
		return
	}
	DPrintf("PutAppend : kv %d is a leader",kv.me)
	kv.mu.Lock()
	ch,ok := kv.kvOp[args.RequestTag]
	if !ok{
		ch = make(chan string,1)
		kv.kvOp[args.RequestTag] = ch
	}
	kv.mu.Unlock()
	DPrintf("PutAppend : kv %d waiting channel",kv.me)
	t := time.Now()
	select {
	case  <- ch:
		//reply. = res
		reply.Err = OK
		DPrintf("PutAppend : kv %d has read the channel",kv.me)
		close(ch)
	case <- time.After(2000*time.Millisecond):
		reply.Err = ErrTimeOut
		DPrintf("kv PutAppend : server = %d time = %v",kv.me,time.Since(t))
		//return
	}
	DPrintf("PutAppend : kv %d has return",kv.me)
}
func (kv *KVServer) readChan()  {
	for kv.killed() == false{
		select {
		case msg := <- kv.applyCh:
			kv.mu.Lock()
			_,isLeader := kv.rf.GetState()
			if msg.CommandValid{
				op := msg.Command.(Op)
				DPrintf("readChan : kv %d op = %v",kv.me,op)
				if n,ok := kv.isDuplicate[op.RequestTag];ok && n > 0{
					DPrintf("readChan : kv %d has a duplicate log = %v",kv.me,op)
					break
				}else if !ok{
					kv.isDuplicate[op.RequestTag] = 1
				}
				//kv.mu.Lock()

				var res string
				//kv.mu.Unlock()
				if op.Operator == "Get"{
					if value,ok := kv.kvMap[op.Key];ok{
						DPrintf("readChan : kv %d pre",kv.me)
						res = value
						DPrintf("readChan : kv %d next",kv.me)
					}else{
						res = ""
					}
				}else{
					if op.Operator == "Append"{
						str,ok := kv.kvMap[op.Key]
						if ok{
							str += op.Value
						}else{
							str = op.Value
						}
						kv.kvMap[op.Key] = str
					}else{
						kv.kvMap[op.Key] = op.Value
					}
					//DPrintf("readChan : kv %d pre chan 's size = %d",kv.me,len(ch))
					res = "js"
					//DPrintf("readChan : kv %d next",kv.me)
				}
				DPrintf("readChan : kv %d has bean write chan",kv.me)
				//kv.isDuplicate[op.RequestTag] = 1
				if isLeader{
					ch,ok := kv.kvOp[op.RequestTag]
					if !ok{
						ch = make(chan string,1)
						kv.kvOp[op.RequestTag] = ch
						DPrintf("readChan : kv %d ch not found",kv.me)
					}
					ch <- res
				}
			}
			kv.mu.Unlock()
			//op = msg.Command.(Op)
		}


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
	kv.kvMap = make(map[string]string)
	kv.kvOp = make(map[Tag]chan string)
	kv.isDuplicate = make(map[Tag]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.readChan()
	// You may need initialization code here.

	return kv
}
