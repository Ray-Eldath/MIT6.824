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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	SnapshotSeq   int
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
	Valid   bool
	Term    int
	Index   int
	Seq     int
	Command interface{}
}

func (e *LogEntry) String() string {
	if e.Valid {
		return fmt.Sprintf("{%d s%d t%d %v}", e.Index, e.Seq, e.Term, e.Command)
	} else {
		return fmt.Sprintf("<%d s%d t%d>", e.Index, e.Seq, e.Term)
	}
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
	votedFor        int
	log             []*LogEntry
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	commitIndex   int
	lastApplied   int
	applyCond     *sync.Cond
	nextIndex     []int
	matchIndex    []int
	replicateCond []*sync.Cond

	applyCh      chan ApplyMsg
	leaseStartAt time.Time
	leaseSyncing bool
}

func (rf *Raft) IsMajority(vote int) bool {
	return vote >= rf.Majority()
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) GetLogAtIndex(logIndex int) *LogEntry {
	if logIndex < rf.log[0].Index {
		return nil
	}
	subscript := rf.LogIndexToSubscript(logIndex)
	if len(rf.log) > subscript {
		return rf.log[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) LogIndexToSubscript(logIndex int) int {
	return logIndex - rf.log[0].Index
}

func (rf *Raft) LogTail() *LogEntry {
	return LogTail(rf.log)
}

func LogTail(xs []*LogEntry) *LogEntry {
	return xs[len(xs)-1]
}

func (rf *Raft) resetTerm(term int) {
	rf.term = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.leaseSyncing = false
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.leaseSyncing = false
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.state == Leader && !rf.leaseSyncing && time.Since(rf.leaseStartAt) < LeaseDuration
	return term, isleader
}

// Deprecated
func (rf *Raft) GetLease() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaseStartAt
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.Debug(dPersist, "persist: %s", rf.FormatStateOnly())
	rf.persister.SaveRaftState(rf.serializeState())
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.term)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []*LogEntry
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if e := d.Decode(&term); e != nil {
		panic(e)
	}
	if e := d.Decode(&votedFor); e != nil {
		panic(e)
	}
	if e := d.Decode(&log); e != nil {
		panic(e)
	}

	rf.Debug(dPersist, "readPersist: t%d votedFor=S%d  current log: %v", term, votedFor, log)
	rf.term = term
	rf.votedFor = votedFor
	rf.log = log
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, lastIncludedSeq int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dSnapshot, "CondInstallSnapshot lastIncludedTerm=%d lastIncludedIndex=%d  %s", lastIncludedTerm, lastIncludedIndex, rf.FormatStateOnly())

	if lastIncludedIndex < rf.commitIndex {
		rf.Debug(dSnapshot, "rejected outdated CondInstallSnapshot")
		return false
	}

	rf.Debug(dSnapshot, "before install snapshot: %s  full log: %v", rf.FormatStateOnly(), rf.FormatFullLog())
	if lastIncludedIndex >= rf.LogTail().Index {
		rf.log = rf.log[0:1]
	} else {
		rf.log = rf.log[rf.LogIndexToSubscript(lastIncludedIndex):]
	}
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Seq = lastIncludedSeq
	rf.log[0].Command = nil
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.Debug(dSnapshot, "after install snapshot: %s  full log: %v", rf.FormatStateOnly(), rf.FormatFullLog())
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(seq int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := len(rf.log) - 1; i >= 0; i-- {
		entry := rf.log[i]
		if entry.Seq == seq {
			rf.Debug(dSnapshot, "snapshot until %d (seq %d)", entry.Index, seq)
			rf.Debug(dSnapshot, "before snapshot:  full log: %v", rf.FormatFullLog())
			rf.log = rf.log[rf.LogIndexToSubscript(entry.Index):]
			rf.log[0].Command = nil
			rf.lastApplied = entry.Index
			rf.Debug(dSnapshot, "after snapshot:  full log: %s", rf.FormatFullLog())
			rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
			return
		}
	}
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

	rf.Debug(dLog, "receive AppendEntries from S%d args.term=%d %+v", args.LeaderId, args.Term, args)
	if rf.state == Candidate && args.Term >= rf.term {
		rf.Debug(dLog, "received term %d >= currentTerm %d from S%d, leader is legitimate", args.Term, rf.term, args.LeaderId)
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	} else if rf.state == Follower && args.Term > rf.term {
		rf.Debug(dLog, "received term %d > currentTerm %d from S%d, reset rf.term", args.Term, rf.term, args.LeaderId)
		rf.resetTerm(args.Term)
	} else if rf.state == Leader && args.Term > rf.term {
		rf.Debug(dLog, "received term %d > currentTerm %d from S%d, back to Follower", args.Term, rf.term, args.LeaderId)
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}

	if args.PrevLogIndex > 0 && args.PrevLogIndex >= rf.log[0].Index {
		if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
			rf.Debug(dLog, "log consistency check failed. local log at prev {%d t%d}: %+v  full log: %s", args.PrevLogIndex, args.PrevLogTerm, prev, rf.FormatFullLog())
			if prev != nil {
				for _, entry := range rf.log {
					if entry.Term == prev.Term {
						reply.ConflictingIndex = entry.Index
						break
					}
				}
				reply.ConflictingTerm = prev.Term
			} else {
				reply.ConflictingIndex = rf.LogTail().Index
				reply.ConflictingTerm = 0
			}
			reply.Success = false

			return
		}
	}
	if len(args.Entries) > 0 {
		// if pass log consistency check, do merge
		rf.Debug(dLog, "before merge: %s", rf.FormatLog())
		if LogTail(args.Entries).Index >= rf.log[0].Index {
			appendLeft := 0
			for i, entry := range args.Entries {
				if local := rf.GetLogAtIndex(entry.Index); local != nil {
					if local.Index != entry.Index {
						panic(rf.Sdebug(dFatal, "LMP violated: local.Index != entry.Index. entry: %+v  local log at entry.Index: %+v", entry, local))
					}
					if local.Term != entry.Term {
						rf.Debug(dLog, "merge conflict at %d", i)
						rf.log = rf.log[:rf.LogIndexToSubscript(entry.Index)]
						appendLeft = i
						break
					}
					appendLeft = i + 1
				}
			}
			for i := appendLeft; i < len(args.Entries); i++ {
				entry := *args.Entries[i]
				rf.log = append(rf.log, &entry)
			}
		}
		rf.Debug(dLog, "after merge: %s", rf.FormatLog())
		rf.persist()
	}
	// trigger apply
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.LogTail().Index)
		rf.applyCond.Broadcast()
	}
	rf.Debug(dLog, "finish process heartbeat: commitIndex=%d", rf.commitIndex)
	reply.Success = true
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

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
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
	if rf.votedFor == -1 {
		rf.Debug(dVote, "S%d RequestVote %+v votedFor=<nil>", args.CandidateId, args)
	} else {
		rf.Debug(dVote, "S%d RequestVote %+v votedFor=S%d", args.CandidateId, args, rf.votedFor)
	}
	if args.Term < rf.term {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		rf.Debug(dVote, "received term %d > currentTerm %d, back to Follower", args.Term, rf.term)
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()
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
		rf.Debug(dVote, "hands down RequestVote from S%d  %+v  full log: %v", args.CandidateId, args, rf.FormatFullLog())
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedSeq   int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	now := time.Now()
	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.Debug(dSnapshot, "received term %d < currentTerm %d from S%d, reject InstallSnapshot")
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = now

	rf.Debug(dSnapshot, "receive InstallSnapshot from S%d args.term=%d LastIncludedIndex=%d LastIncludedSeq=%d LastIncludedTerm=%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedSeq, args.LastIncludedTerm)
	if rf.state == Candidate && args.Term >= rf.term {
		rf.Debug(dSnapshot, "received term %d >= currentTerm %d from S%d, leader is legitimate", args.Term, rf.term, args.LeaderId)
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	} else if rf.state == Follower && args.Term > rf.term {
		rf.Debug(dSnapshot, "received term %d > currentTerm %d from S%d, reset rf.term", args.Term, rf.term, args.LeaderId)
		rf.resetTerm(args.Term)
	} else if rf.state == Leader && args.Term > rf.term {
		rf.Debug(dSnapshot, "received term %d > currentTerm %d from S%d, back to Follower", args.Term, rf.term, args.LeaderId)
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}
	rf.mu.Unlock()

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotSeq:   args.LastIncludedSeq,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

const (
	ElectionTimeoutMax = int64(600 * time.Millisecond)
	ElectionTimeoutMin = int64(500 * time.Millisecond)
	HeartbeatInterval  = 100 * time.Millisecond
	LeaseDuration      = 400 * time.Millisecond
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
			rf.resetTerm(rf.term + 1)
			rf.becomeCandidate()
			rf.votedFor = rf.me
			rf.persist()
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
						rf.becomeFollower()
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
							rf.becomeLeader()
							rf.leaseSyncing = true
							rf.StartL(LogEntry{
								Valid: false,
								Term:  rf.term,
								Index: rf.LogTail().Index + 1,
								Seq:   rf.LogTail().Seq,
							})
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
	rf.Debug(dHeartbeat, "BroadcastHeartbeat start")
	// term := rf.term
	// leaderId := rf.me
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

	lease := &Lease{1, false}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.Replicate(i, lease)
	}

	rf.mu.Lock()
}

func (rf *Raft) DoHeartbeat() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval)
		if !rf.needHeartbeat() {
			continue
		}

		rf.mu.Lock()
		rf.BroadcastHeartbeat()
		rf.mu.Unlock()
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
		if !rf.needApplyL() {
			rf.mu.Unlock()
			continue
		}
		rf.lastApplied += 1
		if rf.leaseSyncing && !rf.needApplyL() {
			rf.leaseSyncing = false
		}
		entry := rf.GetLogAtIndex(rf.lastApplied)
		if entry == nil {
			panic(rf.Sdebug(dFatal, "entry == nil  %s", rf.FormatState()))
		}
		toCommit := *entry
		rf.Debug(dCommit, "apply rf[%d]=%+v", rf.lastApplied, toCommit)
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			Command:      toCommit.Command,
			CommandValid: toCommit.Valid,
			CommandIndex: toCommit.Seq,
		}
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dTrace, "needApply: commitIndex=%d lastApplied=%d", rf.commitIndex, rf.lastApplied)
	return rf.needApplyL()
}

func (rf *Raft) needApplyL() bool {
	return rf.commitIndex > rf.lastApplied
}

type Lease struct {
	leaseVote    int
	leaseUpdated bool
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

		rf.Replicate(peer, &Lease{1, false})
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
func (rf *Raft) Replicate(peer int, lease *Lease) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.log[0].Index {
		var installReply InstallSnapshotReply
		installArgs := &InstallSnapshotArgs{
			Term:              rf.term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log[0].Index,
			LastIncludedSeq:   rf.log[0].Seq,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		installOk := rf.sendInstallSnapshot(peer, installArgs, &installReply)
		if !installOk {
			return
		}

		rf.mu.Lock()
		rf.Debug(dSnapshot, "S%d InstallSnapshotReply %+v rf.term=%d installArgs.Term=%d", peer, installReply, rf.term, installArgs.Term)
		if rf.term != installArgs.Term {
			rf.mu.Unlock()
			return
		}
		if installReply.Term > rf.term {
			rf.Debug(dSnapshot, "return to Follower due to installReply.Term > rf.term")
			rf.becomeFollower()
			rf.resetTerm(installReply.Term)
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[peer] = installArgs.LastIncludedIndex + 1
		rf.mu.Unlock()
		return
	}

	var entries []*LogEntry
	nextIndex := rf.nextIndex[peer]
	for j := nextIndex; j <= rf.LogTail().Index; j++ {
		atIndex := rf.GetLogAtIndex(j)
		if atIndex == nil {
			panic(rf.Sdebug(dFatal, "atIndex == nil  %s", rf.FormatState()))
		}
		entry := *atIndex
		entries = append(entries, &entry)
	}
	prev := rf.GetLogAtIndex(nextIndex - 1)
	rf.Debug(dWarn, "replicate S%d lease=%p nextIndex=%v matchIndex=%v prevLog: %v", peer, lease, rf.nextIndex, rf.matchIndex, prev)
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	// rf.Debug(dReplicate, "replication triggered for S%d with args %+v", peer, args)
	rf.mu.Unlock()

	rf.Sync(peer, args, lease)
}

// Sync must be called W/O rf.mu held.
func (rf *Raft) Sync(peer int, args *AppendEntriesArgs, lease *Lease) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dHeartbeat, "S%d AppendEntriesReply %+v rf.term=%d args.Term=%d lease=%+v", peer, reply, rf.term, args.Term, lease)
	if rf.term != args.Term {
		rf.Debug(dHeartbeat, "S%d AppendEntriesReply rejected since rf.term(%d) != args.Term(%d)", peer, rf.term, args.Term)
		return
	}
	if reply.Term > rf.term {
		rf.Debug(dHeartbeat, "return to Follower due to reply.Term > rf.term")
		rf.becomeFollower()
		rf.resetTerm(reply.Term)
	} else {
		if reply.Success {
			lease.leaseVote++
			if rf.IsMajority(lease.leaseVote) {
				now := time.Now()
				rf.Debug(dHeartbeat, "leaseVote (%d/%d), update lease to %v", lease.leaseVote, len(rf.peers), now)
				rf.leaseStartAt = now
				lease.leaseUpdated = true
			}
			if len(args.Entries) == 0 {
				return
			}
			logTailIndex := LogTail(args.Entries).Index
			rf.matchIndex[peer] = logTailIndex
			rf.nextIndex[peer] = logTailIndex + 1
			rf.Debug(dHeartbeat, "S%d logTailIndex=%d commitIndex=%d lastApplied=%d matchIndex=%v nextIndex=%v", peer, logTailIndex, rf.commitIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex)

			// update commitIndex
			preCommitIndex := rf.commitIndex
			for i := rf.commitIndex; i <= logTailIndex; i++ {
				count := 0
				for p := range rf.peers {
					if rf.matchIndex[p] >= i {
						count += 1
					}
				}
				if rf.IsMajority(count) && rf.GetLogAtIndex(i).Term == rf.term {
					preCommitIndex = i
				}
			}
			rf.commitIndex = preCommitIndex

			// trigger DoApply
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		} else {
			if reply.Term < rf.term {
				rf.Debug(dHeartbeat, "S%d false negative reply rejected")
				return
			}
			nextIndex := rf.nextIndex[peer]
			rf.matchIndex[peer] = 0

			if reply.ConflictingTerm > 0 {
				for i := len(rf.log) - 1; i >= 1; i-- {
					if rf.log[i].Term == reply.ConflictingTerm {
						rf.nextIndex[peer] = Min(nextIndex, rf.log[i].Index+1)
						rf.Debug(dHeartbeat, "S%d old_nextIndex: %d new_nextIndex: %d  full log: %s", peer, nextIndex, rf.nextIndex[peer], rf.FormatFullLog())
						return
					}
				}
			}
			rf.nextIndex[peer] = Max(Min(nextIndex, reply.ConflictingIndex), 1)
			rf.Debug(dHeartbeat, "S%d old_nextIndex: %d new_nextIndex: %d  full log: %s", peer, nextIndex, rf.nextIndex[peer], rf.FormatFullLog())
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
	entry := LogEntry{
		Valid:   true,
		Term:    rf.term,
		Index:   rf.LogTail().Index + 1,
		Seq:     rf.LogTail().Seq + 1,
		Command: command,
	}
	rf.StartL(entry)
	return entry.Seq, rf.term, rf.state == Leader
}

func (rf *Raft) StartL(entry LogEntry) {
	rf.log = append(rf.log, &entry)
	rf.persist()
	rf.matchIndex[rf.me] += 1
	rf.Debug(dClient, "client start replication with entry %v  %s", entry, rf.FormatState())
	//for i := range rf.peers {
	//	if i == rf.me {
	//		continue
	//	}
	//	rf.replicateCond[i].Signal()
	//}
	// use replicateCond[i] (replicate by Sync) could significantly reduce RPC call times since
	// multiple Start will be batched into a single round of replication. but doing so will introduce
	// longer latency because replication is not triggered immediately, and higher CPU time due to
	// serialization is more costly when dealing with longer AppendEntries RPC calls.
	// personally I think the commented Sync design is better, but that wont make us pass speed tests in Lab 3.
	// TODO: how could the actual system find the sweet point between network overhead and replication latency?
	rf.BroadcastHeartbeat()
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
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&sync.Mutex{})

	rf.electionTimeout = NextElectionTimeout()
	rf.log = append(rf.log, &LogEntry{
		Term:    0,
		Index:   0,
		Seq:     0,
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
		if i == rf.me {
			continue
		}
		go rf.DoReplicate(i)
	}
	if len(rf.log) != 1 {
		panic("len(rf.log) != 1")
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
