package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

//用于获取任务
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

//用于worker创建后的注册
type RegArgs struct {
}

type RegReply struct {
	WorkerId int
}

//用于worker响应任务
type ReportTaskArgs struct {
	WorkerId int
	Phase    TaskPhase
	Seq      int
	Done     bool
}

type ReportTaskReply struct {
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
