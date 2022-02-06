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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	if s == Follower {
		return "F"
	} else if s == Candidate {
		return "C"
	} else if s == Leader {
		return "L"
	} else {
		panic("invalid state " + string(rune(s)))
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{%d t%d %v}", e.Index, e.Term, e.Command)
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
	state           State
	term            int
	votedFor        *int
	log             []*LogEntry
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	commitIndex   int
	lastApplied   int
	applyCond     *sync.Cond
	nextIndex     []int
	matchIndex    []int
	replicateCond []*sync.Cond
}

func (rf *Raft) IsMajority(vote int) bool {
	return vote >= rf.Majority()
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) GetLogAtIndex(logIndex int) *LogEntry {
	if len(rf.log) < 2 {
		return nil
	}
	if logIndex < 1 {
		panic("logIndex < 1")
	}
	logicalLogSubscript := logIndex - rf.log[1].Index
	subscript := logicalLogSubscript + 1
	if len(rf.log) > subscript {
		return rf.log[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) LogTail() *LogEntry {
	return LogTail(rf.log)
}

func LogTail(xs []*LogEntry) *LogEntry {
	return xs[len(xs)-1]
}

func (rf *Raft) resetTerm(term int) {
	rf.term = term
	rf.votedFor = nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.state == Leader
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingTerm  int
	ConflictingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.Debug(dLog, "received term %d < currentTerm %d from S%d, reject AppendEntries", args.Term, rf.term, args.LeaderId)
		reply.Success = false
		return
	}
	rf.lastHeartbeat = now

	if rf.state == Candidate {
		if args.Term >= rf.term {
			rf.Debug(dLog, "received term %d >= currentTerm %d from S%d, leader is legitimate", args.Term, rf.term, args.LeaderId)
			rf.state = Follower
			rf.resetTerm(args.Term)
			reply.Success = true
		}
	} else if rf.state == Follower {
		rf.Debug(dLog, "receive AppendEntries from S%d args.term=%d %+v", args.LeaderId, args.Term, args)

		if args.Term > rf.term {
			rf.Debug(dLog, "received term %d > currentTerm %d from S%d, reset rf.term", args.Term, rf.term, args.LeaderId)
			rf.resetTerm(args.Term)
		}
		if args.PrevLogIndex > 0 {
			if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
				rf.Debug(dLog, "log consistency check failed. local log at prev {%d t%d}: %+v  full log: %v", args.PrevLogIndex, args.PrevLogTerm, prev, rf.log)
				if prev != nil {
					conflictingIndex := prev.Index
					for i := prev.Index; i >= 1; i-- {
						if rf.log[i].Term == prev.Term {
							conflictingIndex = rf.log[i].Index
						} else {
							break
						}
					}
					if conflictingIndex > 1 {
						reply.ConflictingIndex = rf.GetLogAtIndex(conflictingIndex - 1).Index
						reply.ConflictingTerm = rf.GetLogAtIndex(reply.ConflictingIndex).Term
					} else {
						reply.ConflictingIndex = 0
						reply.ConflictingTerm = 0 // TODO: may fail here!
					}
				} else {
					reply.ConflictingTerm = rf.LogTail().Index
					reply.ConflictingTerm = rf.LogTail().Term
				}
				reply.Success = false

				return
			}
		}
		if len(args.Entries) > 0 {
			// if pass log consistency check, do merge
			rf.Debug(dLog, "before merge: %s", rf.FormatLog())
			for _, entry := range args.Entries {
				local := rf.GetLogAtIndex(entry.Index)
				if local != nil {
					if local.Index != entry.Index {
						panic(rf.Sdebug(dFatal, "LMP violated: local.Index != entry.Index. headIndex=%d  local log at entry.Index: %+v", rf.log[1].Index, local))
					}
					local.Term = entry.Term
					local.Command = entry.Command
				} else {
					// if rf.log[len(rf.log)-1].Index+1 != entry.Index {
					// 	panic(rf.Sdebug(dFatal, "rf.log[len(rf.log)-1].Index+1 != entry.Index. headIndex=%d", rf.log[1].Index))
					// }
					rf.log = append(rf.log, &LogEntry{
						Term:    entry.Term,
						Index:   entry.Index,
						Command: entry.Command,
					})
				}
			}
			argsTailIndex := LogTail(args.Entries).Index
			if rf.LogTail().Index > argsTailIndex {
				rf.log = rf.log[:argsTailIndex]
			}
			rf.log[0].Index = rf.LogTail().Index
			rf.Debug(dLog, "after merge: %s", rf.FormatLog())
		}
		// trigger apply
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.LogTail().Index)
			rf.applyCond.Broadcast()
		}
		rf.Debug(dLog, "finish process heartbeat: commitIndex=%d", rf.commitIndex)
		reply.Success = true
	} else if rf.state == Leader {
		if args.Term > rf.term {
			rf.Debug(dLog, "received term %d > currentTerm %d from S%d, back to Follower", args.Term, rf.term, args.LeaderId)
			rf.state = Follower
			rf.resetTerm(args.Term)
			reply.Success = true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if rf.votedFor == nil {
		rf.Debug(dVote, "S%d RequestVote %+v votedFor=<nil>", args.CandidateId, args)
	} else {
		rf.Debug(dVote, "S%d RequestVote %+v votedFor=S%d", args.CandidateId, args, *rf.votedFor)
	}
	if args.Term < rf.term {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		rf.Debug(dVote, "received term %d > currentTerm %d, back to Follower", args.Term, rf.term)
		rf.state = Follower
		rf.resetTerm(args.Term)
	}
	if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args) {
		rf.votedFor = &args.CandidateId
		rf.lastHeartbeat = now
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) isAtLeastUpToDate(args *RequestVoteArgs) bool {
	b := false
	if args.LastLogTerm == rf.LogTail().Term {
		b = args.LastLogIndex >= rf.LogTail().Index
	} else {
		b = args.LastLogTerm >= rf.LogTail().Term
	}
	if !b {
		rf.Debug(dVote, "hands down RequestVote from S%d  %+v current log: %s", args.CandidateId, args, rf.FormatLog())
	}
	return b
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

const (
	ElectionTimeoutMax = int64(500 * time.Millisecond)
	ElectionTimeoutMin = int64(300 * time.Millisecond)
	HeartbeatInterval  = 200 * time.Millisecond
)

func NextElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin) // already Millisecond
}

func (rf *Raft) DoElection() {
	for !rf.killed() {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.electionTimeout = NextElectionTimeout()
			rf.term += 1
			rf.state = Candidate
			rf.votedFor = &rf.me
			rf.Debug(dElection, "electionTimeout %dms elapsed, turning to Candidate", rf.electionTimeout/time.Millisecond)

			args := &RequestVoteArgs{
				Term:         rf.term,
				CandidateId:  rf.me,
				LastLogIndex: rf.LogTail().Index,
				LastLogTerm:  rf.LogTail().Term,
			}
			rf.mu.Unlock()

			vote := uint32(1)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int, args *RequestVoteArgs) {
					var reply RequestVoteReply
					ok := rf.sendRequestVote(i, args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.Debug(dElection, "S%d RequestVoteReply %+v rf.term=%d args.Term=%d", i, reply, rf.term, args.Term)
					if rf.state != Candidate || rf.term != args.Term {
						return
					}
					if reply.Term > rf.term {
						rf.Debug(dElection, "return to Follower due to reply.Term > rf.term")
						rf.state = Follower
						rf.resetTerm(reply.Term)
						return
					}
					if reply.VoteGranted {
						rf.Debug(dElection, "<- S%d vote received", i)
						vote += 1

						if rf.IsMajority(int(vote)) {
							for i = 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.LogTail().Index + 1
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = rf.LogTail().Index
							rf.Debug(dLeader, "majority vote (%d/%d) received, turning Leader  %s", vote, len(rf.peers), rf.FormatState())
							rf.BroadcastHeartbeat()
							rf.state = Leader
						}
					}
				}(i, args)
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

// BroadcastHeartbeat must be called with rf.mu held.
func (rf *Raft) BroadcastHeartbeat() {
	rf.Debug(dHeartbeat, "heartbeat start")
	term := rf.term
	leaderId := rf.me
	// leaderCommit := rf.commitIndex
	// for i := range rf.peers {
	// 	if i == leaderId {
	// 		continue
	// 	}
	// 	rf.Debug(dWarn, "try to lock rf.replicateCond[%d].L", i)
	// 	rf.replicateCond[i].L.Lock()
	// 	rf.Debug(dWarn, "locked rf.replicateCond[%d].L", i)
	// }
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == leaderId {
			continue
		}

		go func(peer int) {
			rf.Sync(peer, &AppendEntriesArgs{
				Term:     term,
				LeaderId: leaderId,
				// LeaderCommit: leaderCommit,
				Entries: nil,
			})
			// rf.Debug(dWarn, "sync done. try to unlock replicateCond[%d].L", peer)
			// rf.replicateCond[peer].L.Unlock()
			// rf.Debug(dWarn, "sync done. unlocked replicateCond[%d].L", peer)
		}(i)
	}

	rf.mu.Lock()
}

func (rf *Raft) DoHeartbeat() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval)
		if !rf.needHeartbeat() {
			continue
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.Replicate(i)
		}
	}
}

func (rf *Raft) needHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) DoApply(applyCh chan ApplyMsg) {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		for !rf.needApply() {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}

		rf.mu.Lock()
		rf.lastApplied += 1
		toCommit := *rf.log[rf.lastApplied]
		rf.Debug(dCommit, "apply rf[%d]=%+v  current log: %s", rf.lastApplied, toCommit, rf.FormatLog())
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			Command:      toCommit.Command,
			CommandValid: true,
			CommandIndex: toCommit.Index,
		}
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dTrace, "needApply: commitIndex=%d lastApplied=%d", rf.commitIndex, rf.lastApplied)
	return rf.state != Candidate && rf.commitIndex > rf.lastApplied
}

func (rf *Raft) DoReplicate(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()
	for {
		// rf.Debug(dWarn, "DoReplicate: try to acquire replicateCond[%d].L", peer)
		// rf.Debug(dWarn, "DoReplicate: acquired replicateCond[%d].L", peer)
		for !rf.needReplicate(peer) {
			rf.replicateCond[peer].Wait()
			if rf.killed() {
				return
			}
		}

		rf.Replicate(peer)
	}
}

func (rf *Raft) needReplicate(peer int) bool {
	// rf.Debug(dWarn, "needReplicate: try to acquire mu.Lock")
	rf.mu.Lock()
	// rf.Debug(dWarn, "needReplicate: acquired mu.Lock")
	defer rf.mu.Unlock()
	nextIndex := rf.nextIndex[peer]
	rf.Debug(dTrace, "needReplicate: nextIndex=%v  log tail %+v", rf.nextIndex, rf.LogTail())
	return rf.state == Leader && peer != rf.me && rf.GetLogAtIndex(nextIndex) != nil && rf.LogTail().Index > nextIndex
}

// Replicate must be called W/O rf.mu held.
func (rf *Raft) Replicate(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	var entries []*LogEntry
	nextIndex := rf.nextIndex[peer]
	for j := nextIndex; j < len(rf.log); j++ {
		entry := *rf.log[j]
		entries = append(entries, &entry)
	}
	prevLogIndex := 0
	prevLogTerm := 0
	if nextIndex-1 > 0 {
		prevLogIndex = rf.log[nextIndex-1].Index
		prevLogTerm = rf.log[nextIndex-1].Term
		rf.Debug(dWarn, "replicate S%d nextIndex=%v matchIndex=%v prevLog: %v", peer, rf.nextIndex, rf.matchIndex, rf.GetLogAtIndex(prevLogIndex))
	}
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	// rf.Debug(dReplicate, "replication triggered for S%d with args %+v", peer, args)
	rf.mu.Unlock()

	rf.Sync(peer, args)
}

// Sync must be called W/O rf.mu held.
func (rf *Raft) Sync(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dHeartbeat, "S%d AppendEntriesReply %+v rf.term=%d args.Term=%d", peer, reply, rf.term, args.Term)
	if rf.term != args.Term {
		return
	}
	if reply.Term > rf.term {
		rf.Debug(dHeartbeat, "return to Follower due to reply.Term > rf.term")
		rf.state = Follower
		rf.resetTerm(reply.Term)
	} else {
		if reply.Success {
			if len(args.Entries) == 0 {
				return
			}
			logTailIndex := LogTail(args.Entries).Index
			rf.matchIndex[peer] = logTailIndex
			rf.nextIndex[peer] = logTailIndex + 1
			rf.Debug(dHeartbeat, "S%d logTailIndex=%d commitIndex=%d matchIndex=%v nextIndex=%v", peer, logTailIndex, rf.commitIndex, rf.matchIndex, rf.nextIndex)

			// update commitIndex
			preCommitIndex := rf.commitIndex
			for i := rf.commitIndex; i <= logTailIndex; i++ {
				count := 0
				for p := range rf.peers {
					if rf.matchIndex[p] >= i {
						count += 1
					}
				}
				if rf.IsMajority(count) && rf.log[i].Term == rf.term {
					preCommitIndex = i
				}
			}
			rf.commitIndex = preCommitIndex

			// trigger DoApply
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		} else {
			rf.matchIndex[peer] = 0
			rf.nextIndex[peer] = reply.ConflictingIndex + 1
		}
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.log[0].Index += 1
	rf.log = append(rf.log, &LogEntry{
		Term:    rf.term,
		Index:   rf.log[0].Index,
		Command: command,
	})
	rf.matchIndex[rf.me] += 1
	rf.Debug(dClient, "client start replication with log %s  %s", rf.FormatLog(), rf.FormatStateOnly())
	for i := range rf.peers {
		rf.replicateCond[i].Broadcast()
	}
	return rf.log[0].Index, rf.term, rf.state == Leader
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&sync.Mutex{})

	rf.electionTimeout = NextElectionTimeout()
	rf.log = append(rf.log, &LogEntry{
		Term:    0,
		Index:   0,
		Command: nil,
	})

	go rf.DoElection()
	go rf.DoHeartbeat()
	go rf.DoApply(applyCh)
	for range rf.peers {
		rf.replicateCond = append(rf.replicateCond, sync.NewCond(&sync.Mutex{}))
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	for i := range rf.peers {
		go rf.DoReplicate(i)
	}
	if len(rf.log) != 1 {
		panic("len(rf.log) != 1")
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
