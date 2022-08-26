package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	worerId int
	mapF    func(string, string) []KeyValue
	reduceF func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := worker{
		mapF:    mapf,
		reduceF: reducef,
	}

	worker.register()
	worker.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *worker) run() {
	DPrintf("run")
	for {
		task, err := w.getTask()
		if err != nil {
			DPrintf(err.Error())
			continue
		}
		if !task.Alive {
			DPrintf("worker get task not alive, exit")
			return
		}
		w.doTask(*task)
	}
}

//开始做任务
func (w *worker) doTask(task Task) {
	switch task.Phase {
	case TaskPhase_Map:
		w.doMapTask(task)
	case TaskPhase_Reduce:
		w.doReduceTask(task)
	default:
		panic(fmt.Sprintf("task phase err: %v", task.Phase))
	}
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//map任务时获取要输出的文件名
func (w *worker) getReduceName(mapId, partitionId int) string {
	return fmt.Sprintf("mr-kv-%d-%d", mapId, partitionId)
}

//reduce任务时获取要输出的文件名
func (w *worker) getMergeName(partitionId int) string {
	return fmt.Sprintf("mr-out-%d", partitionId)
}

//做map任务
func (w *worker) doMapTask(task Task) {
	DPrintf("%v start read file %v", w.worerId, task.FileName)
	cont, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		DPrintf("%v", err)
		w.reportTask(task, false)
		return
	}

	kvs := w.mapF(task.FileName, string(cont))
	partions := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partions[pid] = append(partions[pid], kv)
	}

	for k, v := range partions {
		fileName := w.getReduceName(task.Seq, k)
		file, err := os.Create(fileName)
		if err != nil {
			DPrintf("create file-%v fail in doMapTask. %v", fileName, err)
			w.reportTask(task, false)
			return
		}
		encoder := json.NewEncoder(file)
		for _, kv := range v {
			if err := encoder.Encode(&kv); err != nil {
				DPrintf("encode  kvs to file-%v  fail in doMapTask. %v", fileName, err)
				w.reportTask(task, false)
			}
		}
		if err := file.Close(); err != nil {
			DPrintf("close file-%v fail in doMapTask. %v", fileName, err)
			w.reportTask(task, false)
		}
	}
	w.reportTask(task, true)
}

//做reduce任务
func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)

	for i := 0; i < task.NMap; i++ {
		fileName := w.getReduceName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			DPrintf("open  file-%v fail in doReduceTask. %v", fileName, err)
			w.reportTask(task, false)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0)
	for k, v := range maps {
		len := w.reduceF(k, v)
		res = append(res, fmt.Sprintf("%v %v\n", k, len))
	}

	fileName := w.getMergeName(task.Seq)
	if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil {
		DPrintf("write file-%v in doReduceTask. %v", fileName, err)
		w.reportTask(task, false)
	}

	w.reportTask(task, true)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

//rpc请求：注册worker
func (w *worker) register() {
	DPrintf("reg")
	args := &RegArgs{}
	reply := &RegReply{}

	if err := call("Coordinator.RegWorker", args, reply); !err {
		log.Fatal("worker register error!", err)
	}
	w.worerId = reply.WorkerId
}

//rpc请求：请求获取任务
func (w *worker) getTask() (*Task, error) {
	args := TaskArgs{WorkerId: w.worerId}
	reply := TaskReply{}

	if err := call("Coordinator.GetOneTask", &args, &reply); !err {
		return nil, errors.New("worker getTask error!")
	}
	DPrintf("worker get task:%+v", reply.Task)
	return reply.Task, nil
}

//rpc请求：报告任务状态
func (w *worker) reportTask(task Task, done bool) {
	args := ReportTaskArgs{
		WorkerId: w.worerId,
		Phase:    task.Phase,
		Seq:      task.Seq,
		Done:     done,
	}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail:%+v", args)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	conn, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer conn.Close()

	err = conn.Call(rpcname, args, reply) //rpcname = 结构体名.方法名
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
