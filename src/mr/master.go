package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mapcnt int = 0
var reducecnt int = 0

type Master struct {
	// Your definitions here.
	mapFiles    map[string]*task
	reduceFiles map[int][]*task //key : nreduce idx value: reduce tasks
	undoFiles   []*task         //维护没有分配的task 后面加上reduce的单独队列
	undoReduce  []int           //reduce任务分配 以idx存储队列

	reduceDone     map[int]bool //记录reduce任务是否完成
	mapFinished    int          //map任务完成数量
	reduceFinished int          //reduce任务完成数量
	reduceNumber   int          //reduce任务的数量
	mapNumber      int          //map任务的数量
	mu             sync.Mutex
}
type task struct {
	TaskFile     string //file的文件名
	Status       int    //0=未分配 1=已分配进行中 2=已完成 3=超时
	TaskType     int    //0=maptask 1=reducetask
	MapID        int    //分配map id
	ReduceNumber int    //NReduce
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) checkTimeOut(maptask *task, reduceidx int, timeout int) {

	time.Sleep(time.Second * time.Duration(timeout))
	m.mu.Lock()
	defer m.mu.Unlock()

	if maptask.Status == 1 {
		maptask.Status = 0
		m.undoFiles = append(m.undoFiles, maptask)
		fmt.Printf("map addr :%v ,status: %v", maptask, maptask.Status)
		fmt.Print("map task timeout\n")
	} else if reduceidx != -1 {
		fmt.Printf("reduce id : %v , m reduceDone[%v] = %v\n", reduceidx, reduceidx, m.reduceDone[reduceidx])
		if m.reduceDone[reduceidx] == false {
			m.undoReduce = append(m.undoReduce, reduceidx)
			fmt.Printf("reduce : %v", reduceidx)
			fmt.Print("reduce task timeout \n")
		}
	}

}

// get the task
func (m *Master) GetTask(args Args, reply *Reply) error {
	//从undoFiles 里拿task分配
	//当一个work出现异常时（超时）把它负责的任务重新加入undoFiles
	//fmt.Printf("this is a rpc call, undoFiles: %v, mapFinished:%v \n", m.undoFiles, m.mapFinished)
	m.mu.Lock()
	defer m.mu.Unlock()
	//fmt.Printf("m reducefinished: %v , m reduceNumber : %v \n", m.reduceFinished, m.reduceNumber)
	if m.reduceFinished == m.reduceNumber {
		os.Exit(0)
	}

	if m.mapFinished != m.mapNumber && len(m.undoFiles) != 0 {

		nextFile := m.undoFiles[0]
		//get a maptask to reply
		reply.Maptask = nextFile
		reply.Worktype = "map"
		reply.Maptask.Status = 1
		//remove the first maptask from que
		// fmt.Printf("get a map task: %v", reply.Maptask.TaskFile)
		// fmt.Println(reply)
		m.undoFiles = m.undoFiles[1:]
		go m.checkTimeOut(nextFile, -1, 10)
	} else if m.mapFinished == m.mapNumber && m.reduceFinished != m.reduceNumber && len(m.undoReduce) != 0 {
		fmt.Println(m.undoReduce)
		nextReduceid := m.undoReduce[0]
		reply.ReduceID = nextReduceid
		reply.Worktype = "reduce"
		reply.ReduceFiles = m.reduceFiles[nextReduceid]
		m.undoReduce = m.undoReduce[1:]
		//fmt.Printf("get a reduce task: %v", reply.ReduceID)
		//fmt.Println(reply)
		go m.checkTimeOut(&task{Status: -1}, nextReduceid, 10)
	} else {
		reply.Worktype = "wait"
	}
	return nil
}

// single task done
func (m *Master) WorkDone(mes FinishMessage, reply *NoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mes.Ta.TaskType == 0 {
		if mes.Ta.Status == 2 {
			return nil
		}
		m.mapFinished++
		m.mapFiles[mes.Ta.TaskFile].Status = 2
		fmt.Printf("map task : %v has been done , status: %v ,mes Ta addr: %v\n", mes.Ta.TaskFile, mes.Ta.Status, mes.Ta)
		for _, midfile := range mes.Midfiles {
			newreduce := task{TaskFile: midfile, Status: 0,
				TaskType: 1, MapID: mes.Ta.MapID,
				ReduceNumber: mes.Ta.ReduceNumber}
			reduceid, _ := strconv.Atoi(midfile[len(midfile)-1:])
			m.reduceFiles[reduceid] = append(m.reduceFiles[reduceid], &newreduce)

		}

	} else {
		//有一个不等于2 就说明这个任务之前没完成
		if m.reduceDone[mes.Reduceidx] == false {
			m.reduceFinished++
			m.reduceDone[mes.Reduceidx] = true
			fmt.Printf("reduce task: %v has been done \n", mes.Reduceidx)
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.reduceFinished == m.reduceNumber {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapFiles:       make(map[string]*task),
		reduceFiles:    make(map[int][]*task),
		reduceDone:     make(map[int]bool),
		mapFinished:    0,
		reduceFinished: 0,
		reduceNumber:   nReduce,
		mapNumber:      len(files)}

	for _, ta := range files {
		newtask := &task{TaskFile: ta, Status: 0, TaskType: 0, MapID: mapcnt, ReduceNumber: nReduce}
		m.undoFiles = append(m.undoFiles, newtask)
		m.mapFiles[ta] = newtask
		mapcnt++
	}
	for idx := 0; idx < nReduce; idx++ {
		m.undoReduce = append(m.undoReduce, idx)
	}
	m.server()
	return &m
}
