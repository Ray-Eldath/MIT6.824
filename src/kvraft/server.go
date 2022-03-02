package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	serversLen   int

	kv          map[string]string
	kvMu        sync.Mutex
	cid         []int32
	dedup       map[int32]interface{}
	getDone     map[int]chan string
	done        map[int]chan struct{}
	doneMu      sync.Mutex
	lastApplied int
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Debug("readSnapshot: kv=%v", kvmap)
}

func (kv *KVServer) DoApply() {
	for v := range kv.applyCh {
		if kv.killed() {
			return
		}

		if v.CommandValid {
			kv.apply(v)
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			if _, ok := v.Command.(PutAppendArgs); ok {
				kv.doneMu.Lock()
				ch := kv.done[v.CommandIndex]
				kv.doneMu.Unlock()
				// there are several situation when ch could be nil:
				//  1. PutAppend call Start, but raft apply the entry too fast, even before done channel is created.
				//  2. if PutAppend called Start, and before it was pumped out of applyCh the server was rebooted,
				//     done map will be re-initialized, but all old entries will still get out after reboot.
				// in these two cases, we can safely ignore these cases since retry and dedup will fix this.
				if ch != nil {
					ch <- struct{}{}
				}
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

func (kv *KVServer) apply(v raft.ApplyMsg) {
	if v.CommandIndex <= kv.lastApplied {
		kv.Debug("reject ApplyMsg due to smaller Index. lastApplied=%d v=%+v", kv.lastApplied, v)
		return
	}
	kv.kvMu.Lock()
	defer kv.kvMu.Unlock()
	op := v.Command
	var key string
	switch args := op.(type) {
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
		kv.dedup[args.ClientId] = op
		kv.lastApplied = v.CommandIndex
	}
	kv.Debug("applied {%d %+v} value: %s", v.CommandIndex, v.Command, kv.kv[key])
	if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.kvMu.Lock()
	val := kv.kv[args.Key]
	kv.kvMu.Unlock()
	kv.Debug("Get kv[%s]=%s", args.Key, val)
	reply.Value, reply.Err = val, OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := *args
	i, _, isLeader := kv.rf.Start(op)
	kv.Debug("raft start Put i=%d %+v", i, op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan struct{}, 1)
	kv.doneMu.Lock()
	kv.done[i] = ch
	kv.doneMu.Unlock()
	select {
	case <-ch:
		kv.Debug("raft Put done: %+v", op)
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
		return
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
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.serversLen = len(servers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.getDone = make(map[int]chan string)
	kv.done = make(map[int]chan struct{})
	kv.dedup = make(map[int32]interface{})
	kv.kv = make(map[string]string)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.Debug("StartKVServer dedup=%v  kv=%v", kv.dedup, kv.kv)
	go kv.DoApply()

	return kv
}

func init() {
	v := os.Getenv("KV_VERBOSE")
	level := 0
	if v != "" {
		level, _ = strconv.Atoi(v)
	}
	if level < 1 {
		log.SetOutput(ioutil.Discard)
	}
}

const Padding = "    "

func (kv *KVServer) Debug(format string, a ...interface{}) {
	preamble := strings.Repeat(Padding, kv.me)
	epilogue := strings.Repeat(Padding, kv.serversLen-kv.me-1)
	prefix := fmt.Sprintf("%s%s S%d %s[SERVER] ", preamble, raft.Microseconds(time.Now()), kv.me, epilogue)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}
