package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Different stages of MAPREDUCE
const (
	MAP = "map"
	REDUCE = "reduce"
	END = "end"
)

type Task struct {
	TaskType string
	TaskID int
	MapInputFile string
	WorkerID string
	Deadline time.Time
}

// Add your RPC definitions here.
type ApplyTaskArgs struct {
	WorkerID string
}

type ApplyTaskReply struct {
	TaskType string
	TaskID int
	MapInputFile string
	NumMap int
	NumReduce int
}

type ReportTaskArgs struct {
	TaskType string
	TaskID int
	WorkerID string
}

type ReportTaskReply struct {
	OwnsTheTask bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
