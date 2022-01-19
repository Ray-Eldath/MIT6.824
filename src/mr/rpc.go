package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "time"
import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Placeholder struct{}

// ====== FinishTask =======

type FinishArgs struct {
	IsMap bool
	Id    int
}

// ====== Task =======

type TaskState int

const (
	Pending TaskState = iota
	Executing
	Finished
)

type MapTask struct {
	TaskMeta
	Filename string
}

type ReduceTask struct {
	TaskMeta
	IntermediateFilenames []string
}

type TaskMeta struct {
	State     TaskState
	StartTime time.Time
	Id        int
}

type TaskOperation int

const (
	ToWait TaskOperation = iota
	ToRun
)

type Task struct {
	Operation TaskOperation
	IsMap     bool
	NReduce   int
	Map       MapTask
	Reduce    ReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
