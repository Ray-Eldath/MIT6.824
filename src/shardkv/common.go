package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type Args struct {
	ClientId    int32
	SequenceNum int64
}

type PutOrAppend int

const (
	PutOp PutOrAppend = iota
	AppendOp
)

// Put or Append
type PutAppendArgs struct {
	Args
	Type  PutOrAppend
	Key   string
	Value string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Args
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
