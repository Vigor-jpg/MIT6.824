package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)
const Leader int = 0
const Candidate int = 1
const Follower int = 2
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	size int
	currentTerm int
	voteNum int
	voteFor int


	applyCh chan ApplyMsg
	commitIndex int
	lastApplied int

	logs   []LogEntry
	identity int   //身份 Leader Candidate Follower
	recLogCount int

	nextIndex []int
	matchIndex []int

	electionTimer time.Time
	electionTimeout time.Duration

	heartBeat time.Duration
	//2D
	logOffset int
}
type PersistentState struct {
	CurrentTerm int
	VoteFor int
	Logs []LogEntry
	CommitIndex int
	LastReplied int
	//PrevIndex int
	LogOffset int
}
type LogEntry struct {
	Command interface{}
	Term int
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.identity == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VoteFor: rf.voteFor,
		Logs: rf.logs,
		LastReplied: rf.lastApplied,
		CommitIndex: rf.commitIndex,
		LogOffset: rf.logOffset,
	}
	if err := e.Encode(state);err != nil{
		log.Fatal(err)
	}
	DPrintf("getPersistData : -------------------over\n")
	return w.Bytes()
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//log.Printf("-----%d\n",1)
	DPrintf("persist : rf %d logs size == %d,logs == %v,commit = %d,Term = %d\n",rf.me,len(rf.logs),rf.logs,rf.commitIndex,rf.currentTerm)
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
	DPrintf("persist : rf over \n")
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistentState
	// var yyy
	//fmt.Printf("logs size == %d\n",len(rf.logs))
	if err := d.Decode(&state);err != nil{
		log.Fatal(err)
	}else{
		rf.voteFor = state.VoteFor
		rf.logs = make([]LogEntry,0)
		rf.logs = append(rf.logs,state.Logs...)
		rf.currentTerm = state.CurrentTerm
		rf.commitIndex = state.CommitIndex
		rf.lastApplied = state.LastReplied
		rf.logOffset = state.LogOffset
		DPrintf("readPersist : rf %d logs size == %d,logs == %v,commitIndex = %d ,Term = %d\n",rf.me,len(rf.logs),rf.logs,rf.commitIndex,rf.currentTerm)
	}
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludesIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}else if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		//reply.Term = args.Term
		rf.transitToFollower()
	}
	reply.Term = rf.currentTerm
	rf.electionTimer = time.Now()
	msg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludesIndex,
		SnapshotTerm: args.LastIncludedTerm,
		Snapshot: clone(args.Data),
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
}
func (rf *Raft) sendInstallSnapshot(server int)  {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedTerm: rf.logs[0].Term,
		LastIncludesIndex: rf.logOffset,
		Offset: 0,
		Data: rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{
		Term: rf.currentTerm,
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok{
		return
	}
	if reply.Term != args.Term{
		//rf.currentTerm = Max(rf.currentTerm,reply.Term)
		return
	}
	rf.matchIndex[server] = args.LastIncludesIndex
	rf.nextIndex[server] = args.LastIncludesIndex + 1

}
//
// A service wants to switch to snapshot.
//Only do so if Raft hasn't have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > lastIncludedIndex || rf.logs[rf.commitIndex - rf.logOffset].Term > lastIncludedTerm{
		return false
	}
	DPrintf("CondInstallSnapshot : rf %d lastIndex = %d and lastTerm = %d and log size = %d and logs = %v and Term = %d\n",rf.me,lastIncludedIndex,lastIncludedTerm,len(rf.logs),rf.logs,rf.currentTerm)
	//rf.saveSnapShot(lastIncludedIndex,snapshot)
	rf.logs = make([]LogEntry,1)
	rf.logs[0].Term = lastIncludedTerm
	rf.logOffset = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	raftState := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(raftState,snapshot)
	return true
}
func (rf *Raft) saveSnapShot(index int, snapshot []byte){
	DPrintf("snapshot :rf %d log size = %d and trim index = %d\n",rf.me,len(rf.logs) + rf.logOffset,index)
	rf.logs = rf.logs[index - rf.logOffset:]
	rf.logOffset = index
	DPrintf("snapshot :rf %d log size = %d and trim index = %d\n",rf.me,len(rf.logs),index)
	raftState := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(raftState,snapshot)
}
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as
//much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//DPrintf("Snapshot : -------------- start \n")
	//
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.saveSnapShot(index,snapshot)
	//DPrintf("Snapshot : -------------- end \n")
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
	FollowerCommit int
}

func (rf *Raft)AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("AppendEntries : raft %d has been rec the heartbeat from %d\n",rf.me,args.LeaderId)
	reply.Success = true
	rf.electionTimer = time.Now()
	//term不匹配
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		//rf.persist()
		DPrintf("AppendEntries : raft %d term err\n",rf.me)
		return
	}else if args.Term > rf.currentTerm{
		DPrintf("AppendEntries : raft %d has been become a follwer\n",rf.me)
		rf.currentTerm = args.Term
		rf.transitToFollower()
	}
	rf.currentTerm = args.Term
	//logs conflict
	DPrintf("AppendEntries : rf %d log size = %d log offset = %d and args.PreIndex = %d\n",rf.me,len(rf.logs),rf.logOffset,args.PrevLogIndex)
	if len(rf.logs) + rf.logOffset <= args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex - rf.logOffset].Term{
		reply.Success = false
		reply.FollowerCommit = rf.commitIndex
		//rf.logs = rf.logs[:rf.commitIndex+1]
		return
	}
	if len(rf.logs) + rf.logOffset > args.PrevLogIndex && args.PrevLogTerm == rf.logs[args.PrevLogIndex - rf.logOffset].Term{
		//fmt.Printf("rf %d check right -----------------------\n",rf.me)
		//fmt.Printf("rf %d logs length = %d,preindex = %d,preTerm = % d,log.Term = %d\n",rf.me,len(rf.logs) , args.PrevLogIndex , args.PrevLogTerm , rf.logs[args.PrevLogIndex].Term)
		DPrintf("AppendEntries : rf %d commitIndex = %d and leader is %d and log size = %d prevIndex = %d and args.commmitIndex = %dand enties = %v and rf.logs = %v\n",rf.me,rf.commitIndex,args.LeaderId,len(rf.logs),args.PrevLogIndex,args.LeaderCommit,args.Entries,rf.logs)
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.logOffset]
		//fmt.Printf("rf log size = %d\n",len(rf.logs))
		rf.logs = append(rf.logs,args.Entries...)
		reply.Success = true
		reply.Term = rf.currentTerm
		DPrintf("AppendEntries : rf %d commitIndex = %d and log size = %d and args.commmitIndex = %dand enties = %v and rf.logs = %v\n",rf.me,rf.commitIndex,len(rf.logs),args.LeaderCommit,args.Entries,rf.logs)
	}
	if args.LeaderCommit > rf.commitIndex{
		//excute log
		rf.commitIndex = Min(len(rf.logs)+rf.logOffset-1,args.LeaderCommit)
		//rf.logs = rf.logs[:args.PrevLogIndex+1]
		DPrintf("AppendEntries : rf log size = %d and commitIndex = %d\n",len(rf.logs),rf.commitIndex)
		//rf.logs = append(rf.logs,args.Entries...)
		//
		//go rf.commitLogs()
		//fmt.Printf("rf %d Commit = %d and lasReplyed = %d\n",rf.me,rf.commitIndex,rf.lastApplied)
	}
	//rf.mu.Unlock()
}
func (rf *Raft)sendAppendEntries(server int,args *AppendEntriesArgs,reply *AppendEntriesReply) {
	//rf.mu.Lock()
	DPrintf("sendAppendEntries : rf %d sendAppendEntries : has been send the heartbeat to raft %d,msg = %v \n",rf.me,server,args)
	//rf.mu.Lock()

	//rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == false{
		DPrintf("sendAppendEntries : rf %d is disconnected\n",server)
		return
	}
	//rf.mu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.identity != Leader{
		return
	}
	DPrintf("sendAppendEntries : rf %d been send the heartbeat to raft %d\n",args.LeaderId,server)
	DPrintf("sendAppendEntries : reply.Success = %v,reply.Term = %d and Term = %d\n",reply.Success,reply.Term,rf.currentTerm)
	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		DPrintf("sendAppendEntries : leader %d has become a follower\n",rf.me)
		//2022-2-3 bugfix
		//rf.logs更改需谨慎
		//rf.logs = rf.logs[:rf.commitIndex+1]
		rf.transitToFollower()
		//rf.mu.Lock()

		//DPrintf("rf %d has been become a follower\n",rf.me)
		return
	}
	if reply.Success{
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		count := 1
		if rf.commitIndex >= rf.matchIndex[server]{
			return
		}
		for i:=0;i<len(rf.matchIndex);i++{
			if rf.matchIndex[i] >= rf.matchIndex[server]{
				count++
			}
		}
		DPrintf("sendAppendEntries : rf %d args.PreIndex = %d and logOffset = %d\n",rf.me,args.PrevLogIndex,rf.logOffset)
		if count > len(rf.peers)/2 && rf.logs[args.PrevLogIndex + len(args.Entries) - rf.logOffset].Term == rf.currentTerm{
			rf.commitIndex = Max(rf.commitIndex,rf.matchIndex[server])
			//go rf.commitLogs()
		}
	}else{
		rf.nextIndex[server] = reply.FollowerCommit + 1
		//fmt.Printf("false false false false false\n")
		//fmt.Printf("raft %d ---server %d matchIndex = %d nextIndex = %d\n",rf.me,server,rf.matchIndex[server],rf.nextIndex[server])
	}
	//fmt.Printf("rf %d end send append\n",rf.me)


}
func (rf *Raft)sendAppendEntriesToPeers()  {
	for i:= 0;i < len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		if rf.nextIndex[i] <= rf.logOffset{
			go rf.sendInstallSnapshot(i)
			continue
		}
		entriesSize := len(rf.logs) + rf.logOffset - rf.nextIndex[i]
		//fmt.Printf("%d ---- %d \n",len(rf.logs),rf.nextIndex[i])

		entries := make([]LogEntry,0)
		for j := 0;j < entriesSize;j++{
			/*if err := deepCopy(entries[j],rf.logs[rf.nextIndex[i] + j]); err != nil{
				log.Fatal(err)
			}*/
			entry := LogEntry{
				Term: rf.logs[rf.nextIndex[i]+j-rf.logOffset].Term,
				Command: rf.logs[rf.nextIndex[i]+j-rf.logOffset].Command,
			}
			entries = append(entries,entry)
		}
		//fmt.Printf("rf %d -----%d ----nextIndex = %d enties = %v rf.logs = %v\n",i,len(entries),rf.nextIndex[i],entries,rf.logs)
		//fmt.Printf()
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm: rf.logs[rf.nextIndex[i] - 1 - rf.logOffset].Term,
			Entries: entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{
			Success: false,
			Term: rf.currentTerm,
		}
		//fmt.Printf("1111111111111111111111111\n")
		go rf.sendAppendEntries(i,&args,&reply)
	}
}
func (rf *Raft)heartBeats()  {
	for rf.killed() == false{
		//fmt.Printf("raft %d has been send the heartbeat\n",rf.me)
		rf.mu.Lock()
		if rf.identity != Leader{
			//fmt.Printf("rf %d exit the heart loop\n",rf.me)
			rf.mu.Unlock()
			return
		}else{
			//fmt.Printf("leader is %d\n",rf.me)
		}
		//fmt.Printf("rf %d send hear \n",rf.me)
		rf.sendAppendEntriesToPeers()
		rf.mu.Unlock()
		time.Sleep(time.Duration(100)*time.Millisecond)
		//break
	}
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("RequestVote : raft %d has been rec vote from raft %d and voteFor = %d args.Term = %d term = %d\n",rf.me,args.CandidateId,rf.voteFor,args.Term,rf.currentTerm)

	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		DPrintf("RequestVote : raft %d has become a follower\n",rf.me)
		rf.transitToFollower()
	}
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RequestVote : raft %d has granted fail\n",rf.me)
		return
	}
	if  (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(rf.logs[len(rf.logs) - 1].Term < args.LastLogTerm ||
			(rf.logs[len(rf.logs) - 1].Term == args.LastLogTerm && args.LastLogIndex >= len(rf.logs) - 1 + rf.logOffset)){
		DPrintf("RequestVote : raft %d has been vote for raft %d and lastLogIndex = %d ,LastTerm = %d\n",rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//重置时间
		rf.electionTimer = time.Now()
		//rf.logs = rf.logs[:rf.commitIndex]
	}else{
		DPrintf("RequestVote : raft %d ------- raft %d and lastLogIndex = %d ,LastTerm = %d\n",rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply){
	//rf.mu.Lock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//rf.mu.Unlock()
	if ok == false{
		DPrintf("sendRequestVote : rf %d to rf %d is disconnected\n",rf.me,server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.transitToFollower()
		DPrintf("sendRequestVote :rf %d has bean become a follower\n",rf.me)
		return
	}
	if args.Term < rf.currentTerm{
		DPrintf("sendRequestVote : term fail,args.Term = %d,rf.currentTerm = %d\n",args.Term,rf.currentTerm)
		return
	}
	if reply.VoteGranted{
		rf.voteNum++
		DPrintf("sendRequestVote : raft %d has rec a vote from raft %d and Votenum = %d\n",rf.me,server,rf.voteNum)
		if rf.voteNum > len(rf.peers)/2{
			//转换为Leader
			DPrintf("sendRequestVote : raft %d has been become to leader\n",rf.me)
			rf.transitToLeader()
		}
	}
}
func (rf *Raft)sendRequestVoteToPeers()  {
	//fmt.Println("has been rec elect")
	//rf.currentTerm++
	rf.electionTimer = time.Now()
	t := rand.Intn(200)+200
	rf.electionTimeout = time.Duration(t)*time.Millisecond
	for i := 0;i < len(rf.peers);i++{
		if rf.me == i{
			continue
		}
		args := RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogTerm: rf.logs[len(rf.logs)-1].Term,
			LastLogIndex: len(rf.logs)-1+rf.logOffset,
		}
		reply := RequestVoteReply{
		}
		DPrintf("sendRequestVoteToPeers : raft %d has been send vote to raft %d\n",rf.me,i)
		go rf.sendRequestVote(i,&args,&reply)
	}
}

func (rf *Raft)transitToLeader()  {
	//fmt.Printf("rf %d has become a leader\n",rf.me)
	rf.identity = Leader
	//rf.logs = rf.logs[:rf.commitIndex+1]
	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = len(rf.logs) + rf.logOffset
		rf.matchIndex[i] = 0
	}
	rf.electionTimer = time.Now()
	go rf.heartBeats()
}
func (rf *Raft)transitToFollower()  {
	rf.voteFor = -1
	//rf.electionTimer = time.Now()
	rf.identity = Follower
}
func (rf *Raft)transitToCandidate()  {
	rf.voteNum = 1
	rf.voteFor = rf.me
	rf.currentTerm++
	rf.electionTimer = time.Now()
	rf.identity = Candidate
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft)commitLogs()  {
	for rf.killed() == false{
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex{
			rf.lastApplied++
			DPrintf("rf %d commit index = %d  log size = %d and commitIndex = %d and logOffset = %d\n",rf.me,rf.lastApplied,len(rf.logs),rf.commitIndex,rf.logOffset)
			msg := ApplyMsg{CommandValid:true,Command: rf.logs[rf.lastApplied - rf.logOffset].Command,CommandIndex: rf.lastApplied}
			//rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		rf.persist()
		time.Sleep(time.Duration(50)*time.Millisecond)
	}
	//rf.lastApplied = rf.commitIndex
	//fmt.Printf("rf %d has commit log,commitindex = %d\n",rf.me,rf.commitIndex)
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	DPrintf("start : rf %d logs size == %d,logs == %v\n",rf.me,len(rf.logs),rf.logs)
	if rf.identity != Leader{
		rf.mu.Unlock()
		return index,term,false
	}
	logEntry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}
	DPrintf("start : rf %d logs size == %d,logs == %v\n",rf.me,len(rf.logs),rf.logs)
	rf.logs = append(rf.logs,logEntry)
	DPrintf("start : rf %d logs size == %d,logs == %v\n",rf.me,len(rf.logs),rf.logs)
	//go rf.commitLogs()
	rf.recLogCount = 1
	index = len(rf.logs) - 1 + rf.logOffset
	/*for i := 0;i < len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		entriesSize := len(rf.logs) - rf.nextIndex[i];
		//fmt.Printf("%d ---- %d \n",len(rf.logs),rf.nextIndex[i])
		entries := make([]LogEntry,entriesSize)
		for j := 0;j < entriesSize;j++{
			if err := deepCopy(entries[j],rf.logs[rf.nextIndex[i] + j]); err != nil{
				log.Fatal(err)
			}
			entries[j].Term = rf.logs[rf.nextIndex[i] + j].Term
			entries[j].Command = rf.logs[rf.nextIndex[i] + j].Command
		}
		//fmt.Printf("rf %d -----%d ----nextIndex = %d,enties = %v rf.logs = %v\n",i,len(entries),rf.nextIndex[i],entries,rf.logs)
		//fmt.Printf()
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm: rf.logs[rf.nextIndex[i] - 1].Term,
			Entries: entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
		}
		//fmt.Printf("0000000000000000000000000\n")
		go rf.sendAppendEntries(i,&args,&reply)
	}*/
	rf.sendAppendEntriesToPeers()
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	/*t := time.Now()
	for time.Since(t).Seconds() < 3{
		rf.mu.Lock()
		//fmt.Printf("wating------\n")
		if rf.identity != Leader{
			isLeader = false
			rf.mu.Unlock()
			break
		}
		if rf.recLogCount > len(rf.peers)/2 {
			//excute log
			rf.commitIndex = index
			//fmt.Printf("log size = %d\n",len(rf.logs))
			go rf.commitLogs()
			//fmt.Printf("has excute log and commit = %d ---- lastReplied = %d\n",rf.commitIndex,rf.lastApplied)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10)*time.Millisecond)
	}*/
	//rf.mu.Lock()
	/*for i := 0;i<len(rf.peers);i++{
		fmt.Printf("matchIndex = %d and nextIndex = %d\n",rf.matchIndex[i],rf.nextIndex[i])
	}*/

	//rf.mu.Unlock()
	DPrintf("start end index = %d------------------------\n",index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//time.Sleep(time.Duration(t)*time.Microsecond)
		//fmt.Printf("since %d\n",time.Since(rf.electionTimer))
		//fmt.Printf("timeout %d\n",rf.electionTimeout)
		//fmt.Printf("raft %d has been start a election\n",rf.me)
		rf.mu.Lock()
		if time.Since(rf.electionTimer) > rf.electionTimeout && rf.identity != Leader{
			//fmt.Println("start send mesege")
			//fmt.Printf("since %d\n",time.Since(rf.electionTimer))
			//fmt.Printf("timeout %d\n",rf.electionTimeout)
			DPrintf("raft %d has been start a election and Term = %d\n",rf.me,rf.currentTerm+1)
			//rf.mu.Unlock()
			rf.transitToCandidate()
			rf.sendRequestVoteToPeers()
			rf.persist()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	//rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.electionTimer = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(150)+150)*time.Millisecond
	rf.heartBeat = time.Duration(150)
	rf.currentTerm = 1
	rf.voteFor = -1
	rf.lastApplied = 0
	rf.applyCh = applyCh
	//logs
	rf.logs = make([]LogEntry,1)
	rf.logs[0] = LogEntry{
		Term: 0,
		Command: nil,
	}
	rf.logOffset = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int,len(peers))
	rf.nextIndex = make([]int,len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("make init1 : rf %d logs size == %d,logs == %v\n",rf.me,len(rf.logs),rf.logs)

	rf.identity = Follower
	//rf.logs = append(initLog,rf.logs...)
	// start ticker goroutine to start elections
	//rf.mu.Unlock()
	DPrintf("make : rf %d logs size == %d,logs == %v,commitIndex = %d\n",rf.me,len(rf.logs),rf.logs,rf.commitIndex)
	go rf.ticker()
	go rf.commitLogs()
	return rf
}