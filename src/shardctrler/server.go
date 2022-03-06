package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "fmt"
import "io/ioutil"
import "log"
import "math"
import "os"
import "sort"
import "strconv"
import "strings"
import "time"


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
		c := (max - min) / 2
		ts := ct[maxi][0:c]
		sc.LeaderDebug("%d max=%d maxi=%d min=%d mini=%d ts=%v", conf.Num, max, maxi, min, mini, ts)
		ct[maxi] = ct[maxi][c:]
		ct[mini] = append(ct[mini], ts...)
		for _, t := range ts {
			conf.Shards[t] = mini
		}
		sc.LeaderDebug("%d ct=%v conf.Shards=%v", conf.Num, ct, conf.Shards)
	}
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
	if len(ct[0]) > 0 { // empty ct[0] before doing anything else
		if min == math.MaxInt {
			return 0, 0, 0, 0
		} else {
			return len(ct[0]) * 2, 0, 0, mini
		}
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
	sc.mu.Lock()
	sc.Debug("applied {%d %+v} configs: %+v", v.CommandIndex, v.Command, sc.configs)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Join", *args)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Leave", *args)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Move", *args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	num := args.Num
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if num < 0 || num > sc.ConfigsTailL().Num {
		num = sc.ConfigsTailL().Num
	}
	reply.WrongLeader = false
	reply.Err = OK
	reply.Config = sc.configs[num]
	sc.Debug("Query done: config[%d]=%+v ConfigsTail=%+v", num, reply.Config, sc.ConfigsTailL())
}

const TimeoutInterval = 500 * time.Millisecond

// Command args needs to be raw type (not pointer)
func (sc *ShardCtrler) Command(ty string, args interface{}) (i int, wrongLeader bool, err Err) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		return -1, true, OK
	}
	i, _, isLeader := sc.rf.Start(args)
	sc.Debug("raft start %s i=%d %+v", ty, i, args)
	if !isLeader {
		return -1, true, OK
	}
	ch := make(chan int, 1)
	sc.mu.Lock()
	sc.done[i] = ch
	sc.mu.Unlock()
	select {
	case v := <-ch:
		sc.Debug("raft %s done: %+v ConfigsTail=%+v", ty, args, sc.ConfigsTail())
		return v, false, OK
	case <-time.After(TimeoutInterval):
		return -1, false, ErrTimeout
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
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.serversLen = len(servers)
	sc.done = make(map[int]chan int)
	sc.dedup = make(map[int32]interface{})
	go sc.DoApply()

	return sc
}

const Padding = "    "

var quiet bool

func (sc *ShardCtrler) Debug(format string, a ...interface{}) {
	if quiet {
		return
	}
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
	_, quiet = os.LookupEnv("SHARDCTL_QUIET")
}
