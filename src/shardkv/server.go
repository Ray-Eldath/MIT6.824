package shardkv

import "6.824/labrpc"
import "6.824/raft"
import "sync"
import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"strings"
	"time"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	mck          *shardctrler.Clerk
	config       shardctrler.Config
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	serversLen   int

	kv          map[string]string
	dedup       map[int32]interface{}
	done        map[int]chan string
	lastApplied int
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	var dedup map[int32]interface{}
	var kvmap map[string]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&dedup); e == nil {
		kv.dedup = dedup
	}
	if e := d.Decode(&kvmap); e == nil {
		kv.kv = kvmap
	}
}

func (kv *ShardKV) DoApply() {
	for v := range kv.applyCh {
		if v.CommandValid {
			if latest, ok := v.Command.(shardctrler.Config); ok {
				kv.mu.Lock()
				current := kv.config
				kv.Debug("%d DoUpdateConfig current=%+v latest=%+v", kv.gid, current, latest)
				handoff := make(map[int][]int) // gid -> shards
				kv.config.Groups = latest.Groups
				for shard, gid := range current.Shards {
					target := latest.Shards[shard]
					if gid == kv.gid && target != kv.gid { // move from self to others
						handoff[target] = append(handoff[target], shard)
						kv.config.Shards[shard] = target
					} else if gid == 0 && latest.Shards[shard] == kv.gid { // move from 0 to self
						kv.config.Shards[shard] = kv.gid
					}
				}

				if kv.isLeader() {
					for gid, shards := range handoff {
						slice := make(map[string]string)
						for key := range kv.kv {
							for _, shard := range shards {
								if key2shard(key) == shard {
									slice[key] = kv.kv[key]
								}
							}
						}
						kv.Debug("%d handoff shards %v to gid %d: %v", kv.gid, shards, gid, slice)
						if servers, ok := latest.Groups[gid]; ok {
							go kv.handoff(shards, slice, gid, servers)
						} else {
							panic("no group to handoff")
						}
					}
				}
				kv.mu.Unlock()
			} else {
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
					kv.Debug("checkSnapshot: kv.rf.GetStateSize(%d) >= kv.maxraftstate(%d)", kv.rf.GetStateSize(), kv.maxraftstate)
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					kv.mu.Lock()
					if err := e.Encode(kv.dedup); err != nil {
						panic(err)
					}
					if err := e.Encode(kv.kv); err != nil {
						panic(err)
					}
					kv.mu.Unlock()
					kv.rf.Snapshot(v.CommandIndex, w.Bytes())
				}
				val, applied := kv.apply(v)
				if !kv.isLeader() || !applied {
					continue
				}
				kv.mu.Lock()
				ch := kv.done[v.CommandIndex]
				kv.mu.Unlock()
				go func() {
					ch <- val
				}()
			}
		} else if v.SnapshotValid {
			b := kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.SnapshotSeq, v.Snapshot)
			kv.Debug("CondInstallSnapshot %t SnapshotTerm=%d SnapshotIndex=%d SnapshotSeq=%d len(Snapshot)=%d", b, v.SnapshotTerm, v.SnapshotIndex, v.SnapshotSeq, len(v.Snapshot))
			if b {
				kv.lastApplied = v.SnapshotSeq
				kv.readSnapshot(v.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) apply(v raft.ApplyMsg) (string, bool) {
	if v.CommandIndex <= kv.lastApplied {
		kv.Debug("reject ApplyMsg due to smaller Index. lastApplied=%d v=%+v", kv.lastApplied, v)
		return "", false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		if !kv.checkInGroupL(key) {
			kv.Debug("reject ApplyMsg due to not in group  v=%+v", v)
			return "", false
		}
		kv.LeaderDebug("%d applied Get {%d %v} value: %s config: %+v", kv.gid, v.CommandIndex, v.Command, kv.kv[key], kv.config)
		break
	case PutAppendArgs:
		key = args.Key
		if !kv.checkInGroupL(key) {
			kv.Debug("reject ApplyMsg due to not in group  v=%+v", v)
			return "", false
		}
		if dup, ok := kv.dedup[args.ClientId]; ok {
			if putDup, ok := dup.(PutAppendArgs); ok && putDup.RequestId == args.RequestId {
				kv.Debug("duplicate found for putDup=%+v  args=%+v", putDup, args)
				break
			}
		}
		if args.Type == PutOp {
			kv.kv[key] = args.Value
		} else {
			kv.kv[key] += args.Value
		}
		kv.dedup[args.ClientId] = v.Command
		kv.LeaderDebug("%d applied PutAppend {%d %+v} value: %s config: %+v", kv.gid, v.CommandIndex, v.Command, kv.kv[key], kv.config)
		break
	case HandoffArgs:
		kv.Debug("%d applying Handoff from gid %d: %+v config: %+v", kv.gid, args.Origin, args, kv.config)
		for k, v := range args.Kv {
			kv.kv[k] = v
		}
		for _, shard := range args.Shards {
			kv.config.Shards[shard] = kv.gid
		}
	}
	kv.lastApplied = v.CommandIndex
	if key != "" {
		return kv.kv[key], true
	} else {
		return "", true
	}
}

const UpdateConfigInterval = 100 * time.Millisecond

func (kv *ShardKV) DoUpdateConfig() {
	for {
		time.Sleep(UpdateConfigInterval)
		if !kv.isLeader() {
			continue
		}
		kv.rf.Start(kv.mck.Query(-1))
	}
}

func (kv *ShardKV) handoff(shards []int, slice map[string]string, target int, servers []string) {
	for {
		for _, si := range servers {
			var reply HandoffReply
			args := HandoffArgs{kv.gid, shards, slice}
			ok := kv.sendHandoff(si, &args, &reply)
			if ok && reply.Err == OK {
				kv.mu.Lock()
				kv.LeaderDebug("%d handoff %v to %d done", kv.gid, shards, target)
				for k := range args.Kv {
					delete(kv.kv, k)
				}
				kv.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type HandoffArgs struct {
	Origin int
	Shards []int
	Kv     map[string]string
}

type HandoffReply struct {
	Err Err
}

func (kv *ShardKV) Handoff(args *HandoffArgs, reply *HandoffReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	_, reply.Err = kv.startAndWait("Handoff", *args)
}

func (kv *ShardKV) sendHandoff(si string, args *HandoffArgs, reply *HandoffReply) bool {
	return kv.make_end(si).Call("ShardKV.Handoff", args, reply)
}

const TimeoutInterval = 500 * time.Millisecond

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	v, err := kv.Command("Get", args.Key, *args)
	reply.Err = err
	if err == OK {
		reply.Value = v
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, reply.Err = kv.Command("PutAppend", args.Key, *args)
}

// Command args needs to be raw type (not pointer)
func (kv *ShardKV) Command(ty string, key string, args interface{}) (val string, err Err) {
	if !kv.isLeader() {
		return "", ErrWrongLeader
	}
	if !kv.checkInGroup(key) {
		return "", ErrWrongGroup
	}
	return kv.startAndWait(ty, args)
}

// startAndWait args needs to be raw type (not pointer)
func (kv *ShardKV) startAndWait(ty string, cmd interface{}) (val string, err Err) {
	i, _, isLeader := kv.rf.Start(cmd)
	kv.mu.Lock()
	kv.Debug("%d raft start %s i=%d %+v  config: %+v", kv.gid, ty, i, cmd, kv.config)
	if !isLeader {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	ch := make(chan string, 1)
	kv.done[i] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		kv.Debug("%d raft %s done: %+v => %v", kv.gid, ty, cmd, v)
		return v, OK
	case <-time.After(TimeoutInterval):
		return "", ErrTimeout
	}
}

func (kv *ShardKV) checkInGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.checkInGroupL(key)
}

func (kv *ShardKV) checkInGroupL(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(HandoffArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.serversLen = len(servers)
	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kv = make(map[string]string)
	kv.dedup = make(map[int32]interface{})
	kv.done = make(map[int]chan string)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.Debug("%d StartServer dedup=%v  kv=%v", gid, kv.dedup, kv.kv)
	go kv.DoApply()
	go kv.DoUpdateConfig()

	return kv
}

func (kv *ShardKV) LeaderDebug(format string, a ...interface{}) {
	if !kv.isLeader() {
		kv.Debug(format, a...)
	}
}

const Padding = "    "

func (kv *ShardKV) Debug(format string, a ...interface{}) {
	preamble := strings.Repeat(Padding, kv.me)
	epilogue := strings.Repeat(Padding, kv.serversLen-kv.me-1)
	prefix := fmt.Sprintf("%s%s S%d %s[SHARDKV] ", preamble, raft.Microseconds(time.Now()), kv.me, epilogue)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}
