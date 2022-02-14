package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Args interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kv      map[string]string
	cid     []int32
	dedup   map[int32]Op
	getDone map[int]chan string
	putDone map[int]chan struct{}
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
			kv.mu.Lock()
			log.Printf("S%d apply {%d %+v}", kv.me, v.CommandIndex, v.Command)
			switch args := v.Command.(Op).Args.(type) {
			case GetArgs:
				getCh := kv.getDone[v.CommandIndex]
				val := ""
				if s, ok := kv.kv[args.Key]; ok {
					val = s
				}
				kv.mu.Unlock()
				go func() {
					getCh <- val
				}()
				break
			case PutAppendArgs:
				putCh := kv.putDone[v.CommandIndex]
				kv.mu.Unlock()
				go func() {
					putCh <- struct{}{}
				}()
			}
		} else {
			//TODO: call CondSnapshot
		}
	}
}

func (kv *KVServer) apply(v raft.ApplyMsg) {
	op := v.Command.(Op)
	switch args := op.Args.(type) {
	case GetArgs:
		break
	case PutAppendArgs:
		if dup, ok := kv.dedup[args.ClientId]; ok {
			if putDup, ok := dup.Args.(PutAppendArgs); ok && putDup.RequestId == args.RequestId {
				log.Printf("duplicate found for putDup=%+v  args=%+v", putDup, args)
				return
			}
		}
		if args.Type == PutOp {
			kv.kv[args.Key] = args.Value
		} else {
			kv.kv[args.Key] += args.Value
		}
		kv.dedup[args.ClientId] = op
	}
}

const TimeoutInterval = 500 * time.Millisecond

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{*args}
	i, _, isLeader := kv.rf.Start(op)
	log.Printf("S%d raft start Get i=%d %+v", kv.me, i, op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.getDone[i] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		log.Printf("S%d raft Get done: %+v => %+v", kv.me, op, v)
		reply.Value = v
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{*args}
	i, _, isLeader := kv.rf.Start(op)
	log.Printf("S%d raft start Put i=%d %+v", kv.me, i, op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.putDone[i] = ch
	kv.mu.Unlock()
	select {
	case <-ch:
		log.Printf("S%d raft Put done: %+v", kv.me, op)
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
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.getDone = make(map[int]chan string)
	kv.putDone = make(map[int]chan struct{})
	kv.dedup = make(map[int32]Op)
	kv.kv = make(map[string]string)
	go kv.DoApply()

	return kv
}

func init() {
	if raft.DebugVerbosity < 1 {
		log.SetOutput(ioutil.Discard)
	}
}
