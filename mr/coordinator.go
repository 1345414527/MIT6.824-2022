package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskPhase int  //任务阶段
type TaskStatus int //任务状态

//任务阶段
const (
	TaskPhase_Map    TaskPhase = 0
	TaskPhase_Reduce TaskPhase = 1
)

//任务状态
const (
	TaskStatus_New        TaskStatus = 0 //还没有创建
	TaskStatus_Ready      TaskStatus = 1 //进入队列
	TaskStatus_Running    TaskStatus = 2 //已经分配，正在运行
	TaskStatus_Terminated TaskStatus = 3 //运行结束
	TaskStatus_Error      TaskStatus = 4 //运行出错
)

const (
	ScheduleInterval   = time.Millisecond * 500 //扫描任务状态的间隔时间
	MaxTaskRunningTime = time.Second * 5        //每个任务的最大执行时间，用于判断是否超时
)

//任务
type Task struct {
	FileName string    //当前任务的文件名
	Phase    TaskPhase //当前任务状态
	Seq      int       //当前的任务序列
	NMap     int       //map任务/file的数量
	NReduce  int       //reduce任务/分区的数量
	Alive    bool      //是否存活
}

//任务状态
type TaskState struct {
	Status    TaskStatus //任务状态
	WorkerId  int        //执行当前Task的workerid
	StartTime time.Time  //任务开始执行的时间
}

type Coordinator struct {
	files      []string    //存储要处理的文件
	nReduce    int         //reduce/分区数量
	taskPhase  TaskPhase   //任务阶段
	taskStates []TaskState //任务的状态
	taskChan   chan Task   //任务队列
	workerSeq  int         //worker序列
	done       bool        //是否做完
	muLock     sync.Mutex  //互斥锁

}

//创建一个task
func (c *Coordinator) NewOneTask(seq int) Task {
	task := Task{
		FileName: "",
		Phase:    c.taskPhase,
		NMap:     len(c.files),
		NReduce:  c.nReduce,
		Seq:      seq,
		Alive:    true,
	}

	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", c, seq, len(c.files), len(c.taskStates))

	if task.Phase == TaskPhase_Map {
		task.FileName = c.files[seq]
	}
	return task
}

//扫描任务状态并适当更新
func (c *Coordinator) scanTaskState() {
	DPrintf("scanTaskState...")
	c.muLock.Lock()
	defer c.muLock.Unlock()

	//这里不能使用函数Done()，因为此时已经上锁
	if c.done {
		return
	}

	allDone := true
	//循环每个任务的状态
	for k, v := range c.taskStates {
		switch v.Status {
		case TaskStatus_New:
			allDone = false
			c.taskStates[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		case TaskStatus_Ready:
			allDone = false
		case TaskStatus_Running:
			allDone = false
			//超时重新分配该任务
			if time.Now().Sub(v.StartTime) > MaxTaskRunningTime {
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			}
		case TaskStatus_Terminated:
		case TaskStatus_Error:
			allDone = false
			c.taskStates[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		default:
			panic("t. status err in schedule")
		}
	}

	//如果当前任务完成了
	if allDone {
		if c.taskPhase == TaskPhase_Map {
			//进入Reduce阶段
			DPrintf("init ReduceTask")
			c.taskPhase = TaskPhase_Reduce
			c.taskStates = make([]TaskState, c.nReduce)
		} else {
			log.Println("finish all tasks!!!😊")
			c.done = true
		}
	}
}

//定时更新状态
func (c *Coordinator) schedule() {
	for !c.Done() {
		c.scanTaskState()
		time.Sleep(ScheduleInterval)
	}
}

// Your code here -- RPC handlers for the worker to call.

//处理Rpc请求：获取任务
func (c *Coordinator) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskChan
	reply.Task = &task

	if task.Alive {
		//修改状态
		c.muLock.Lock()
		if task.Phase != c.taskPhase {
			return errors.New("GetOneTask Task phase neq")
		}
		c.taskStates[task.Seq].WorkerId = args.WorkerId
		c.taskStates[task.Seq].Status = TaskStatus_Running
		c.taskStates[task.Seq].StartTime = time.Now()
		c.muLock.Unlock()
	}

	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

//处理Rpc请求：注册worker
func (c *Coordinator) RegWorker(args *RegArgs, reply *RegReply) error {
	DPrintf("worker reg!")
	c.muLock.Lock()
	defer c.muLock.Unlock()
	c.workerSeq++
	reply.WorkerId = c.workerSeq
	return nil
}

//处理Rpc请求：worker响应task完成情况
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, c.taskPhase)

	//如果发现阶段不同或者当前任务已经分配给了其它worker就不修改当前任务状态
	if c.taskPhase != args.Phase || c.taskStates[args.Seq].WorkerId != args.WorkerId {
		DPrintf("in report task,workerId=%v report a useless task=%v", args.WorkerId, args.Seq)
		return nil
	}

	if args.Done {
		c.taskStates[args.Seq].Status = TaskStatus_Terminated
	} else {
		c.taskStates[args.Seq].Status = TaskStatus_Error
	}

	go c.scanTaskState()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)  // 注册 RPC 服务
	rpc.HandleHTTP() // 将 RPC 服务绑定到 HTTP 服务中去
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//如果工作全部完成，返回true
//
func (c *Coordinator) Done() bool {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		taskPhase:  TaskPhase_Map,
		taskStates: make([]TaskState, len(files)),
		workerSeq:  0,
		done:       false,
	}
	if len(files) > nReduce {
		c.taskChan = make(chan Task, len(files))
	} else {
		c.taskChan = make(chan Task, nReduce)
	}

	go c.schedule()
	c.server()
	DPrintf("master init")

	return &c
}
