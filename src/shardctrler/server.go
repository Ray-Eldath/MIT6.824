package shardctrler

import "6.824/raft"
import "6.824/labrpc"
import (
	"6.824/labgob"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs     []Config // indexed by config num
	dedup       map[int32]interface{}
	done        map[int]chan int
	lastApplied int
	serversLen  int
}

func (sc *ShardCtrler) ConfigsTail() Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) ConfigsTailL() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) conf() Config {
	var conf Config
	conf.Groups = make(map[int][]string)
	conf.Num = sc.ConfigsTailL().Num + 1
	for k, v := range sc.ConfigsTailL().Groups {
		conf.Groups[k] = v
	}
	for i, s := range sc.ConfigsTailL().Shards {
		conf.Shards[i] = s
	}
	return conf
}

func (sc *ShardCtrler) rebalance(conf *Config) {
	ct := map[int][]int{0: {}} // gid -> shard
	var gs []int
	for gid := range conf.Groups {
		ct[gid] = []int{}
		gs = append(gs, gid)
	}
	sort.Ints(gs)
	for i, gid := range conf.Shards {
		ct[gid] = append(ct[gid], i)
	}
	sc.LeaderDebug("%d ct=%v", conf.Num, ct)
	for max, maxi, min, mini := maxmin(ct, gs); max-min > 1; max, maxi, min, mini = maxmin(ct, gs) {
		sc.LeaderDebug("%d max=%d maxi=%d min=%d mini=%d", conf.Num, max, maxi, min, mini)
		t := ct[maxi][0]
		ct[maxi] = ct[maxi][1:]
		ct[mini] = append(ct[mini], t)
		conf.Shards[t] = mini
	}
	sc.LeaderDebug("%d rebalanced: shards=%v ct=%v", conf.Num, conf.Shards, ct)
}

func maxmin(ct map[int][]int, gs []int) (max int, maxi int, min int, mini int) {
	max = 0
	min = math.MaxInt
	for _, g := range gs {
		l := len(ct[g])
		if l > max {
			max = l
			maxi = g
		}
		if l < min {
			min = l
			mini = g
		}
	}
	if len(ct[0]) > 0 {
		return math.MaxInt, 0, min, mini
	} else {
		return max, maxi, min, mini
	}
}

func (sc *ShardCtrler) DoApply() {
	for v := range sc.applyCh {
		if v.CommandValid {
			sc.apply(v)
			if _, isLeader := sc.rf.GetState(); !isLeader {
				continue
			}
			sc.mu.Lock()
			ch := sc.done[v.CommandIndex]
			num := -1
			switch cmd := v.Command.(type) {
			case QueryArgs:
				num = cmd.Num
				if num < 0 || num > sc.ConfigsTailL().Num {
					num = sc.ConfigsTailL().Num
				}
				break
			}
			sc.mu.Unlock()
			go func() {
				ch <- num
			}()
		}
	}
}

func (sc *ShardCtrler) apply(v raft.ApplyMsg) {
	if v.CommandIndex <= sc.lastApplied {
		sc.Debug("reject ApplyMsg due to smaller Index. lastApplied=%d v=%+v", sc.lastApplied, v)
		return
	}
	var args Args
	switch cmd := v.Command.(type) {
	case JoinArgs:
		args = cmd.Args
		if dup, ok := sc.dedup[cmd.ClientId]; ok {
			if joinDup, ok := dup.(JoinArgs); ok && joinDup.RequestId == cmd.RequestId {
				sc.Debug("duplicate found for joinDup=%+v  cmd=%+v", joinDup, cmd)
				return
			}
		}
		conf := sc.conf()
		for k, v := range cmd.Servers {
			conf.Groups[k] = v
		}
		sc.rebalance(&conf)
		sc.mu.Lock()
		sc.configs = append(sc.configs, conf)
		sc.mu.Unlock()
		break
	case LeaveArgs:
		args = cmd.Args
		if dup, ok := sc.dedup[cmd.ClientId]; ok {
			if leaveDup, ok := dup.(LeaveArgs); ok && leaveDup.RequestId == cmd.RequestId {
				sc.Debug("duplicate found for leaveDup=%+v  cmd=%+v", leaveDup, cmd)
				return
			}
		}
		conf := sc.conf()
		for _, k := range cmd.GIDs {
			for i, gid := range conf.Shards {
				if gid == k {
					conf.Shards[i] = 0
				}
			}
			delete(conf.Groups, k)
		}
		sc.LeaderDebug("LeaveArgs cmd=%+v conf=%+v", cmd, conf)
		sc.rebalance(&conf)
		sc.mu.Lock()
		sc.configs = append(sc.configs, conf)
		sc.mu.Unlock()
		break
	case MoveArgs:
		args = cmd.Args
		if dup, ok := sc.dedup[cmd.ClientId]; ok {
			if moveDup, ok := dup.(MoveArgs); ok && moveDup.RequestId == cmd.RequestId {
				sc.Debug("duplicate found for moveDup=%+v  cmd=%+v", moveDup, cmd)
				return
			}
		}
		conf := sc.conf()
		conf.Shards[cmd.Shard] = cmd.GID
		sc.mu.Lock()
		sc.configs = append(sc.configs, conf)
		sc.mu.Unlock()
	}
	sc.dedup[args.ClientId] = v.Command
	sc.lastApplied = v.CommandIndex
	sc.Debug("applied {%d %+v}", v.CommandIndex, v.Command)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.WrongLeader, reply.Err = sc.Command("Join", *args)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.WrongLeader, reply.Err = sc.Command("Leave", *args)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.WrongLeader, reply.Err = sc.Command("Move", *args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := *args
	i, _, isLeader := sc.rf.Start(op)
	sc.Debug("raft start Query i=%d %+v", i, op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan int, 1)
	sc.mu.Lock()
	sc.done[i] = ch
	sc.mu.Unlock()
	select {
	case v := <-ch:
		sc.mu.Lock()
		conf := sc.configs[v]
		sc.mu.Unlock()
		sc.Debug("raft Query done: op %+v => configs[%d] => %+v", op, v, conf)
		reply.Config = conf
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
		return
	}
}

const TimeoutInterval = 500 * time.Millisecond

// Command args needs to be raw type (not pointer)
func (sc *ShardCtrler) Command(ty string, args interface{}) (wrongLeader bool, err Err) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		return true, OK
	}
	i, _, isLeader := sc.rf.Start(args)
	sc.Debug("raft start %s i=%d %+v", ty, i, args)
	if !isLeader {
		return true, OK
	}
	ch := make(chan int, 1)
	sc.mu.Lock()
	sc.done[i] = ch
	sc.mu.Unlock()
	select {
	case <-ch:
		sc.Debug("raft %s done: %+v ConfigsTail=%+v", ty, args, sc.ConfigsTail())
		return false, OK
	case <-time.After(TimeoutInterval):
		return false, ErrTimeout
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.serversLen = len(servers)
	sc.done = make(map[int]chan int)
	sc.dedup = make(map[int32]interface{})
	go sc.DoApply()

	return sc
}

const Padding = "    "

func (sc *ShardCtrler) Debug(format string, a ...interface{}) {
	preamble := strings.Repeat(Padding, sc.me)
	epilogue := strings.Repeat(Padding, sc.serversLen-sc.me-1)
	prefix := fmt.Sprintf("%s%s S%d %s[SERVER] ", preamble, raft.Microseconds(time.Now()), sc.me, epilogue)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}

func (sc *ShardCtrler) LeaderDebug(format string, a ...interface{}) {
	if _, isLeader := sc.rf.GetState(); isLeader {
		sc.Debug(format, a...)
	}
}

func init() {
	v := os.Getenv("SHARDKV_VERBOSE")
	level := 0
	if v != "" {
		level, _ = strconv.Atoi(v)
	}
	if level < 1 {
		log.SetOutput(ioutil.Discard)
	}
}
