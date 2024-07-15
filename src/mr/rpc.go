package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	CallType string
}

type Reply struct {
	Worktype    string  // map or reduce or wait
	Maptask     *task   //map task
	ReduceFiles []*task //reduce tasks for this reduce worker
	ReduceID    int
}

// finish message
type FinishMessage struct {
	Ta        *task
	Midfiles  []string // only for map tasks
	Reduceidx int      //only for reduce task
}
type NoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
