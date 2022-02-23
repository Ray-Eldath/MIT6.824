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

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	var dedup map[int32]interface{}
	var kvmap map[string]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&dedup); e != nil {
		dedup = make(map[int32]interface{})
	}
	if e := d.Decode(&kvmap); e != nil {
		kvmap = make(map[string]string)
	}
	kv.dedup = dedup
	kv.kv = kvmap
}

func (kv *ShardKV) DoApply() {
	for v := range kv.applyCh {
		if v.CommandValid {
			kv.apply(v)
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.Lock()
			ch := kv.done[v.CommandIndex]
			val := ""
			switch args := v.Command.(type) {
			case GetArgs:
				if s, ok := kv.kv[args.Key]; ok {
					val = s
				}
				break
			}
			kv.mu.Unlock()
			go func() {
				ch <- val
			}()
		} else {
			b := kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.Snapshot)
			kv.Debug("CondInstallSnapshot %t SnapshotTerm=%d SnapshotIndex=%d len(Snapshot)=%d", b, v.SnapshotTerm, v.SnapshotIndex, len(v.Snapshot))
			if b {
				kv.lastApplied = v.SnapshotIndex
				kv.readSnapshot(v.Snapshot)
			}
		}
	}
}

const UpdateConfigInterval = 1000 * time.Millisecond

func (kv *ShardKV) DoUpdateConfig() {
	for {
		time.Sleep(UpdateConfigInterval)
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) apply(v raft.ApplyMsg) {
	if v.CommandIndex <= kv.lastApplied {
		kv.Debug("reject ApplyMsg due to smaller Index. lastApplied=%d v=%+v", kv.lastApplied, v)
		return
	}
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		kv.lastApplied = v.CommandIndex
		break
	case PutAppendArgs:
		key = args.Key
		if dup, ok := kv.dedup[args.ClientId]; ok {
			if putDup, ok := dup.(PutAppendArgs); ok && putDup.RequestId == args.RequestId {
				kv.Debug("duplicate found for putDup=%+v  args=%+v", putDup, args)
				break
			}
		}
		if args.Type == PutOp {
			kv.kv[args.Key] = args.Value
		} else {
			kv.kv[args.Key] += args.Value
		}
		kv.dedup[args.ClientId] = v.Command
		kv.lastApplied = v.CommandIndex
	}
	kv.Debug("applied {%d %+v} value: %s", v.CommandIndex, v.Command, kv.kv[key])
	if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
		kv.Debug("checkSnapshot: kv.rf.GetStateSize(%d) >= kv.maxraftstate(%d)", kv.rf.GetStateSize(), kv.maxraftstate)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if err := e.Encode(kv.dedup); err != nil {
			panic(err)
		}
		if err := e.Encode(kv.kv); err != nil {
			panic(err)
		}
		kv.rf.Snapshot(v.CommandIndex, w.Bytes())
	}
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
	if !kv.checkInGroup(key) {
		return "", ErrWrongGroup
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return "", ErrWrongLeader
	}
	i, _, isLeader := kv.rf.Start(args)
	kv.Debug("%d raft start %s i=%d %+v", kv.gid, ty, i, args)
	if !isLeader {
		return "", ErrWrongLeader
	}
	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.done[i] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		kv.Debug("%d raft %s done: %+v => %v", kv.gid, ty, args, v)
		return v, OK
	case <-time.After(TimeoutInterval):
		return "", ErrTimeout
	}
}

func (kv *ShardKV) checkInGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.config.Groups[kv.config.Shards[key2shard(key)]]
	return ok
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
	kv.done = make(map[int]chan string)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.Debug("%d StartServer dedup=%v  kv=%v", gid, kv.dedup, kv.kv)
	go kv.DoApply()
	go kv.DoUpdateConfig()

	return kv
}

const Padding = "    "

func (kv *ShardKV) Debug(format string, a ...interface{}) {
	preamble := strings.Repeat(Padding, kv.me)
	epilogue := strings.Repeat(Padding, kv.serversLen-kv.me-1)
	prefix := fmt.Sprintf("%s%s S%d %s[SHARDKV] ", preamble, raft.Microseconds(time.Now()), kv.me, epilogue)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}
