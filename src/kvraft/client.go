package kvraft

import "6.824/labrpc"
import "crypto/rand"
import rand2 "math/rand"
import "math/big"
import "time"
import "sync/atomic"
import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	cid     int32
	leader  int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = rand2.Int31()
	return ck
}

func (ck *Clerk) args() Args {
	return Args{ClientId: ck.cid, RequestId: nrand()}
}

const RetryInterval = 300 * time.Millisecond

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	leader := atomic.LoadInt32(&ck.leader)
	for {
		var reply GetReply
		log.Printf("CID%d Client call Get leader=%d", ck.cid, leader)
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrTimeout {
				continue
			}
		}
		leader = ck.nextLeader(leader)
		time.Sleep(RetryInterval)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key: key, Value: value, Args: ck.args()}
	if op == "Put" {
		args.Type = PutOp
	} else {
		args.Type = AppendOp
	}
	leader := atomic.LoadInt32(&ck.leader)
	for {
		var reply PutAppendReply
		log.Printf("CID%d Client call PutAppend leader=%d", ck.cid, leader)
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout {
				continue
			}
		}
		leader = ck.nextLeader(leader)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) nextLeader(current int32) int32 {
	next := (current + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leader, next)
	return next
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
