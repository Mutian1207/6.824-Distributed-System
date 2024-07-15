package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	for {
		var reply *Reply = AskJob()

		switch reply.Worktype {
		case "wait":
			{
				time.Sleep(time.Second * time.Duration(10))
			}
		case "map":
			{
				HandleMap(reply, mapf)
			}
		case "reduce":
			{
				HandleReduce(reply, reducef)
			}
		}
		time.Sleep(time.Second * time.Duration(1))
	}
}

// ask for job  rpc to master
func AskJob() *Reply {
	reply := Reply{}
	args := Args{"request"}
	work := call("Master.GetTask", &args, &reply)
	if work == false {
		os.Exit(0)
	}
	// fmt.Println(reply)

	return &reply
}

// tell the master task finished
func WorkDone(mes FinishMessage) {

	call("Master.WorkDone", mes, &NoneReply{})
}

// handle map
func HandleMap(reply *Reply, mapf func(string, string) []KeyValue) {
	filename := reply.Maptask.TaskFile
	task := reply.Maptask
	task.Status = 1
	// fmt.Printf("Map : working on file: %v \n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	midFile := make([][]KeyValue, reply.Maptask.ReduceNumber)
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.Maptask.ReduceNumber
		midFile[idx] = append(midFile[idx], kv)
	}
	name_X := reply.Maptask.MapID
	var midfiles []string
	for idx, _ := range midFile {
		name_Y := idx
		StoreMidFile(midFile[idx], name_X, name_Y)
		midfiles = append(midfiles, "mr-"+strconv.Itoa(name_X)+"-"+strconv.Itoa(name_Y))
	}
	mes := FinishMessage{task, midfiles, -1}
	defer WorkDone(mes)

}

// store middle file to mr-X-Y
func StoreMidFile(kva []KeyValue, name_X int, name_Y int) {

	fname := "mr-" + strconv.Itoa(name_X) + "-" + strconv.Itoa(name_Y)
	file, err := os.Create(fname)

	defer file.Close()
	if err != nil {
		log.Fatalf("cannot operate on %v", fname)
	}
	enc := json.NewEncoder(file) // 储存json格式文件
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}

}

// load midfile for reduce part
// handle one file return []KeyValue
func LoadReduceFiles(reduceID int, reducetask *task) []KeyValue {
	filename := reducetask.TaskFile
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// handle reduce
func HandleReduce(reply *Reply, reducef func(string, []string) string) {
	var kva []KeyValue
	// load all files kva structs to this kva
	for _, task := range reply.ReduceFiles {
		task.Status = 1
		kva = append(kva, LoadReduceFiles(reply.ReduceID, task)...)
	}

	sort.Sort(ByKey(kva))

	//create reduce output file
	filename := "mr-out-" + strconv.Itoa(reply.ReduceID)
	ofile, _ := os.Create(filename)
	defer ofile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	mes := FinishMessage{&task{TaskType: 1}, []string{}, reply.ReduceID}
	defer WorkDone(mes)

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
