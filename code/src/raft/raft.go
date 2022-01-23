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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as
//
//
//
//much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	//fmt.Printf("raft %d has been rec the heartbeat from %d\n",rf.me,args.LeaderId)
	reply.Success = true
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		//fmt.Printf("term err\n")
		return
	}else{
		//fmt.Printf("raft %d has been become a follwer\n",rf.me)
		rf.transitToFollower()
	}
	rf.currentTerm = args.Term
	if len(rf.logs) <= args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term{
		reply.Success = false
		reply.FollowerCommit = rf.commitIndex
		rf.logs = rf.logs[:rf.commitIndex+1]
		return
	}

	if args.LeaderCommit > rf.commitIndex{
		//excute log
		rf.commitIndex = Min(len(rf.logs)-1,args.LeaderCommit)
		//rf.logs = rf.logs[:args.PrevLogIndex+1]
		//fmt.Printf("rf log size = %d\n",len(rf.logs))
		//rf.logs = append(rf.logs,args.Entries...)
		go rf.commitLogs()
		//fmt.Printf("rf %d Commit = %d and lasReplyed = %d\n",rf.me,rf.commitIndex,rf.lastApplied)
	}

	if len(args.Entries) == 0{
		//fmt.Printf("rf %d heart heart \n",rf.me)
		return
	}else if len(rf.logs) > args.PrevLogIndex && args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term{
		//fmt.Printf("rf %d check right -----------------------\n",rf.me)
		//fmt.Printf("rf %d logs length = %d,preindex = %d,preTerm = % d,log.Term = %d\n",rf.me,len(rf.logs) , args.PrevLogIndex , args.PrevLogTerm , rf.logs[args.PrevLogIndex].Term)

		rf.logs = rf.logs[:args.PrevLogIndex+1]
		//fmt.Printf("rf log size = %d\n",len(rf.logs))
		rf.logs = append(rf.logs,args.Entries...)
		reply.Success = true
		reply.Term = rf.currentTerm
		//fmt.Printf("rf %d commitIndex = %d and args.commmitIndex = %dand enties = %v and rf.logs = %v\n",rf.me,rf.commitIndex,args.LeaderCommit,args.Entries,rf.logs)
		/*if args.LeaderCommit > rf.commitIndex{
			//excute log
			rf.commitIndex = Min(len(rf.logs)-1,args.LeaderCommit)
			//rf.logs = rf.logs[:args.PrevLogIndex+1]
			//fmt.Printf("rf log size = %d\n",len(rf.logs))
			//rf.logs = append(rf.logs,args.Entries...)
			go rf.commitLogs()
			fmt.Printf("rf %d Commit = %d and lasReplyed = %d\n",rf.me,rf.commitIndex,rf.lastApplied)
		}*/
		//fmt.Printf("rf %d log size = %d\n",rf.me,len(rf.logs))
		//
	}else{
		//fmt.Printf("rf %d logs length = %d,preindex = %d\n",rf.me,len(rf.logs),args.PrevLogIndex)
		reply.Success = false
		//reply.Term = rf.currentTerm
	}
	//rf.mu.Unlock()
}
func (rf *Raft)sendAppendEntries(server int,args *AppendEntriesArgs,reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//fmt.Printf("been send the heartbeat to raft %d\n",server)
	//rf.mu.Lock()

	//rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == false{
		return
	}
	//rf.mu.Unlock()
	rf.mu.Lock()
	if rf.identity != Leader{
		rf.mu.Unlock()
		return
	}
	//fmt.Printf("rf %d been send the heartbeat to raft %d\n",args.LeaderId,server)
	//fmt.Printf("reply.Success = %v,reply.Term = %d\n",reply.Success,reply.Term)
	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		//fmt.Printf("leader %d has become a follower\n",rf.me)
		rf.logs = rf.logs[:rf.commitIndex+1]
		rf.transitToFollower()
		rf.mu.Unlock()
		//rf.mu.Lock()
		//fmt.Printf("rf %d has been lock\n",rf.me)
		return
	}
	if reply.Success{
		if len(args.Entries) != 0 {
			//rf.recLogCount++
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			count := 1
			for i:=0;i<len(rf.matchIndex);i++{
				if rf.matchIndex[i] >= rf.matchIndex[server]{
					count++
				}
			}
			if count > len(rf.peers)/2{
				rf.commitIndex = Max(rf.commitIndex,rf.matchIndex[server])
				go rf.commitLogs()
			}
			//fmt.Printf("server %d matchIndex = %d nextIndex = %d\n",server,rf.matchIndex[server],rf.nextIndex[server])
		}
	}else{
		rf.nextIndex[server] = reply.FollowerCommit + 1
		//fmt.Printf("false false false false false\n")
		//fmt.Printf("raft %d ---server %d matchIndex = %d nextIndex = %d\n",rf.me,server,rf.matchIndex[server],rf.nextIndex[server])
	}
	//fmt.Printf("rf %d end send append\n",rf.me)
	rf.mu.Unlock()
}
func (rf *Raft)sendAppendEntriesToPeers()  {
	for i:= 0;i < len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		entriesSize := len(rf.logs) - rf.nextIndex[i]
		//fmt.Printf("%d ---- %d \n",len(rf.logs),rf.nextIndex[i])

		entries := make([]LogEntry,0)
		for j := 0;j < entriesSize;j++{
			/*if err := deepCopy(entries[j],rf.logs[rf.nextIndex[i] + j]); err != nil{
				log.Fatal(err)
			}*/
			entry := LogEntry{
				Term: rf.logs[rf.nextIndex[i]+j].Term,
				Command: rf.logs[rf.nextIndex[i]+j].Command,
			}
			entries = append(entries,entry)
		}
		//fmt.Printf("rf %d -----%d ----nextIndex = %d enties = %v rf.logs = %v\n",i,len(entries),rf.nextIndex[i],entries,rf.logs)
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
	//fmt.Printf("raft %d has been rec vote from raft %d and voteFor = %d\n",rf.me,args.CandidateId,rf.voteFor)

	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.transitToFollower()
	}
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if  (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(rf.logs[len(rf.logs) - 1].Term < args.LastLogTerm ||
			(rf.logs[len(rf.logs) - 1].Term == args.LastLogTerm && args.LastLogIndex >= len(rf.logs) - 1)){
		//fmt.Printf("raft %d has been vote for raft %d and lastLogIndex = %d ,LastTerm = %d\n",rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//rf.logs = rf.logs[:rf.commitIndex]
	}else{
		//fmt.Printf("raft %d ------- raft %d and lastLogIndex = %d ,LastTerm = %d\n",rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.transitToFollower()
		//fmt.Printf("has bean ")
		return
	}
	if args.Term < rf.currentTerm{
		//fmt.Printf("term fail")
		return
	}
	if ok && reply.VoteGranted{
		rf.voteNum++
		//fmt.Printf("raft %d has rec a vote from raft %d and Votenum = %d\n",rf.me,server,rf.voteNum)

		if rf.voteNum > len(rf.peers)/2{
			//转换为Leader
			//fmt.Printf("raft %d has been become to leader\n",rf.me)
			rf.transitToLeader()
		}
	}
}
func (rf *Raft)sendRequestVoteToPeers()  {
	//fmt.Println("has been rec elect")
	rf.currentTerm++
	rf.electionTimer = time.Now()
	t := rand.Intn(200)+100
	rf.electionTimeout = time.Duration(t)*time.Millisecond
	for i := 0;i < len(rf.peers);i++{
		if rf.me == i{
			continue
		}
		args := RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogTerm: rf.logs[len(rf.logs)-1].Term,
			LastLogIndex: len(rf.logs)-1,
		}
		reply := RequestVoteReply{
		}
		//fmt.Printf("raft %d has been send vote to raft %d",rf.me,i)
		go rf.sendRequestVote(i,&args,&reply)
	}
}

func (rf *Raft)transitToLeader()  {
	//fmt.Printf("rf %d has become a leader\n",rf.me)
	rf.identity = Leader
	rf.logs = rf.logs[:rf.commitIndex+1]
	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.electionTimer = time.Now()
	go rf.heartBeats()
}
func (rf *Raft)transitToFollower()  {
	rf.voteFor = -1
	rf.electionTimer = time.Now()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1;i<=rf.commitIndex;i++{
		msg := ApplyMsg{CommandValid:true,Command: rf.logs[i].Command,CommandIndex: i}
		//fmt.Printf("rf %d commit index = %d and msg == %v\n",rf.me,i,msg)
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
	//fmt.Printf("rf %d has commit log,commitindex = %d\n",rf.me,rf.commitIndex)
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	if rf.identity != Leader{
		rf.mu.Unlock()
		return index,term,false
	}
	logEntry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs,logEntry)
	//go rf.commitLogs()
	rf.recLogCount = 1
	index = len(rf.logs) - 1
	for i := 0;i < len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		entriesSize := len(rf.logs) - rf.nextIndex[i];
		//fmt.Printf("%d ---- %d \n",len(rf.logs),rf.nextIndex[i])
		entries := make([]LogEntry,entriesSize)
		for j := 0;j < entriesSize;j++{
			/*if err := deepCopy(entries[j],rf.logs[rf.nextIndex[i] + j]); err != nil{
				log.Fatal(err)
			}*/
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
	}
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
	rf.mu.Lock()
	/*for i := 0;i<len(rf.peers);i++{
		fmt.Printf("matchIndex = %d and nextIndex = %d\n",rf.matchIndex[i],rf.nextIndex[i])
	}*/
	term = rf.currentTerm
	rf.mu.Unlock()
	//fmt.Printf("start end index = %d------------------------\n",index)
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
			//fmt.Printf("raft %d has been start a election\n",rf.me)
			//rf.mu.Unlock()
			rf.transitToCandidate()
			rf.sendRequestVoteToPeers()
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
	rf.logs = make([]LogEntry,1)
	initEntry := LogEntry{
		Term: 0,
	}
	rf.logs[0] = initEntry
	rf.matchIndex = make([]int,len(peers))
	rf.nextIndex = make([]int,len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections

	if me == 0{
		rf.transitToLeader()
	}else{
		rf.identity = Follower
	}
	//rf.mu.Unlock()
	go rf.ticker()
	return rf
}