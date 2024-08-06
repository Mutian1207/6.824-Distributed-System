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
	"bytes"
	"main/labrpc"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log Entry structure
// Each log entry contains a command for state machine, and the term when entry was received by leader (first index is 1)
// Index is the position of the entry in the log
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means vote is granted
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader index in peers
	PrevLogIndex int        // index of log entry before the next new log
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
	AppendIndex  int
}

// AppendEntries reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower accepted the entries
}

// A Go object implementing a single Raft peer.
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// Raft is the structure that holds the state of the server
// It also holds the persistent state of the server
// It also holds the volatile state of the server
// It also holds the volatile state of the leader (if any)
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* Persistent state on all servers */
	currentTerm int        // persistent state on all servers
	votedFor    int        // candidateId that received vote in the current term (or null if none)
	log         []LogEntry // array of log entries

	/* Volatile state on all servers */
	commitIndex     int       // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied     int       // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastTimeHeardHB time.Time // last time heartbeat was received or heard vote message from other candidates

	electionTimeout time.Duration
	state           RaftState
	/* Volatile state on leaders */
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyCh    chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// get a random election time out durantion
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	return time.Duration(200+rand.Intn(150)) * time.Millisecond
}

// reset last time heartbeat
func (rf *Raft) resetHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastTimeHeardHB = time.Now()

}

// periodically check the election time out
func (rf *Raft) checkElectionTimeout() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader && time.Since(rf.lastTimeHeardHB) > rf.electionTimeout {
			rf.lastTimeHeardHB = time.Now()

			DPrintf("Now Raft %v 's election timeout || starting election", rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)

	}
}

// start a new election
func (rf *Raft) startElection() {

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	// 用新的线程 开始选举 避免死锁 checkTimeout里加了锁需要尽快返回解锁
	go rf.requestVote()

}

// become follower
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// become leader
func (rf *Raft) becomeLeader() {
	DPrintf(" Raft %v has become leader", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.votedFor = -1
	go rf.sendHeartbeats()
}

// become leader)
// send RequestVote to all peers
func (rf *Raft) requestVote() {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	voteCnt := 1                                 // initialized to 1 = vote for self
	voteChan := make(chan bool, len(rf.peers)-1) // for communicating and managing all goroutines
	//for loop request to all peer servers
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if i != rf.me {
			go func(i int) {
				// gid := getGID()
				reply := &RequestVoteReply{}
				rf.mu.Lock()
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				ok := rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf("Raft %v is sending vote request to Raft %v , goroutineID: %v, Raft %v Locked", rf.me, i, gid, rf.me)
				// 如果身份还是candidate 并且没有任何其他raft服务器 term比请求的currentTerm大 则还能继续发送申请
				if ok && rf.state == Candidate && args.Term == rf.currentTerm {
					if reply.VoteGranted {
						voteChan <- true
					} else if reply.Term > rf.currentTerm {
						DPrintf(" Raft %v's term < Raft %v's term, now become a follower", rf.me, i)
						rf.becomeFollower(reply.Term)
						voteChan <- false
					}
				} else {
					voteChan <- false
				}
				// DPrintf(" Raft %v has sent vote request to Raft %v , goroutineID: %v, Raft %v Unlocked!", rf.me, i, gid, rf.me)
			}(i)

		}
	}
	// voteGranted > len(peers) //2  - > become leader
	// else convert to follower

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case vote := <-voteChan:
			// DPrintf(" Raft %v get one vote !  main process id %v , now Locked ", rf.me, getGID())
			rf.mu.Lock()
			if !vote && rf.state == Follower {
				// DPrintf(" Raft %v requesting vote became follower , get Unlocked and return!  main process id %v", rf.me, getGID())
				rf.mu.Unlock()
				return
			} else if vote {
				voteCnt++
				if voteCnt > len(rf.peers)/2 && rf.state == Candidate {

					rf.becomeLeader()
					// 获得大多数投票 如果还有peer的term > currentTerm 在后续发送hb时更正
					rf.mu.Unlock()
					// DPrintf(" Raft %v has become leader , get unlocked and return ! main process id %v", rf.me, getGID())
					return
				}

			}

			rf.mu.Unlock()

		// selection timeout == fail
		case <-time.After(300 * time.Millisecond):
			// 超时没收到大多数服务器回应
			rf.mu.Lock()
			rf.becomeFollower(rf.currentTerm)
			rf.mu.Unlock()
			return
		}
	}

}

// peers handle vote request from candidate
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// gid := getGID()
	// DPrintf("Raft %v is handling vote request from Raft %v, goroutineID: %v ,Raft %v is locked", rf.me, args.CandidateId, gid, rf.me)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	} else {
		// 如果候选人有更高的term 则应该先更新状态
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1) 这样才能给新的term的候选人投票

		if rf.currentTerm < args.Term {
			rf.becomeFollower(args.Term)
		}

		// log should up-to-date
		// 1. candidate last log term > rf.last log term ✅
		// 2. candidate last log term == rf.last log term && candidate last log index >= rf.last log index ✅
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.lastTimeHeardHB = time.Now()
			}
		} else {
			reply.VoteGranted = false
		}
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := // `rf` is a struct representing a single Raft peer in a distributed system. It contains fields
		// for managing the state of the Raft server, such as mutex for synchronization, RPC endpoints
		// for communication with other peers, a Persister for persisting state, the index of the peer
		// in the list of peers, and a flag for indicating if the peer is dead.
		rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).\
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	// 如果是leader 先追加在本地log 发布Appendentries
	newEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	repIdx := len(rf.log)
	rf.log = append(rf.log, newEntry)
	// DPrintf("Leader %v has log: %v", rf.me, rf.log)
	DPrintf("Raft %v (leader) has appended newEntry %+v to local", rf.me, command)
	go rf.startReplication(repIdx)

	return index, term, isLeader
}

// 实现更严格的 commitIndex 更新 for server
func (rf *Raft) updateCommitIndex() {
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if rf.log[n].Term == rf.currentTerm {
			count := 1
			for i := range rf.peers {
				//多数服务器match了 append过后才算计数
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				go rf.applyToStateMachine()
			}
		}
	}
}
func (rf *Raft) startReplication(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.nextIndex[i] <= index {
			go rf.replicateToFollower(i)
		}
	}
}

func (rf *Raft) replicateToFollower(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		prevLogIndex := rf.nextIndex[peer] - 1
		if prevLogIndex >= len(rf.log) {
			rf.mu.Unlock()
			return
		}
		DPrintf("raft %v has log : %v", rf.me, rf.log)
		entries := rf.log[rf.nextIndex[peer]:]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		if rf.sendAppendEntries(peer, args, &reply) {
			rf.mu.Lock()
			if reply.Success {
				rf.nextIndex[peer] = prevLogIndex + len(entries) + 1
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				rf.updateCommitIndex()
				rf.mu.Unlock()
				return
			} else if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			} else {
				// 日志不匹配，减少 nextIndex 并重试
				rf.nextIndex[peer] = max(1, rf.nextIndex[peer]-1)
				rf.mu.Unlock()
				continue
			}
		} else {
			// RPC 失败，稍后重试
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to add cleanup code
// here.
// leader periodically send heartbeats to follower
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					// The above code is a comment in Go programming language. It is using the double forward slashes
					// "//" to indicate a single-line comment. The comment is providing information about the action
					// being taken in the following code line, which is sending an empty heartbeat from one Raft
					// instance to another.
					// DPrintf("Raft %v is sending empty heartbeat to Raft %v", rf.me, i)
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if ok {
						if reply.Term > rf.currentTerm {
							DPrintf("Leader Raft %v's current term %v is < raft %v's term %v , now become a follower",
								rf.me, rf.currentTerm, i, reply.Term)
							rf.becomeFollower(reply.Term)
						}
					}

				}(i)
			}

		}
		time.Sleep(100 * time.Millisecond)
	}

}

// leader send heartbeats to follower

// send heartbeats
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

// receive and handle heartbeats
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// DPrintf("Raft %v has log: %v", rf.me, rf.log)

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm > args.Term {
		return
	}

	rf.lastTimeHeardHB = time.Now()

	// 日志一致性检查
	if len(rf.log) <= args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		DPrintf("raft %v 日志提交不一致, log : %v", rf.me, rf.log)
		return
	}

	// 追加新的日志条目
	// 保留已经匹配的日志条目。
	// 只替换或追加不匹配或新的日志条目。

	insertIndex := args.PrevLogIndex + 1
	newlogIndex := 0
	//找到第一个不匹配的日志index
	for ; insertIndex < len(rf.log) && newlogIndex < len(args.Entries); insertIndex, newlogIndex = insertIndex+1, newlogIndex+1 {
		if rf.log[insertIndex].Term != args.Entries[newlogIndex].Term {
			break
		}
	}
	//如果没有不匹配的地方那么说明newEntry已被写在log
	//如果有不匹配的地方 则log 在insertIndex后面都要删除并写上新的entry
	if newlogIndex < len(args.Entries) {
		rf.log = append(rf.log[:insertIndex], args.Entries[newlogIndex:]...)
		rf.persist()
		DPrintf("Raft %d log len: %v ", rf.me, len(rf.log))
	}

	// 更新 commitIndex 一般heartbeat时会更新
	// 需要等到leader 提交过后follower 才能提交 由heartbeat触发

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyToStateMachine()
	}
	// DPrintf(" Raft %v has commitIndex :%v, log: %v ,log len:%v, pre log index: %v",
	// 	rf.me, rf.commitIndex, rf.log, len(rf.log), args.PrevLogIndex)
	reply.Success = true
}

// apply to state machine
func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// respond to server after apply
	for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {

		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}

		DPrintf(" Raft %v is applying log %v to state machine and responding, msg:%v", rf.me, i, applyMsg)
		rf.applyCh <- applyMsg
		rf.lastApplied = i
	}
	// DPrintf(" Raft %v 's lastapplied idx is %v", rf.me, rf.lastApplied)
	// TODO apply to state machine

}

// the service or tester wants to kill a Raft server. the supplies
// the port number of the Raft server in peers[].
// n.b. don't worry if command arrives when the server is dead.

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.electionTimeout = rf.getRandomElectionTimeout()
	rf.state = Follower
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Index: 0}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.resetHeartbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// DPrintf(" Raft %v is initialized ! ", me)
	go rf.checkElectionTimeout()
	// go rf.sendHeartbeats() //最好在成为leader后再开启线程资源
	return rf
}
