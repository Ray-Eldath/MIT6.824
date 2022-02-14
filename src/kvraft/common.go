package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type RegisterClientArgs struct{}

type RegisterClientReply struct {
	ClientId int32
	Err      Err
}

type Args struct {
	ClientId  int32
	RequestId int64
}

type PutOrAppend int

const (
	PutOp PutOrAppend = iota
	AppendOp
)

// Put or Append
type PutAppendArgs struct {
	Type  PutOrAppend
	Key   string
	Value string
	Args
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Args
}

type GetReply struct {
	Err   Err
	Value string
}
