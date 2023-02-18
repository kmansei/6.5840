package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	UNDONE TaskStatus = iota
	EXECUTING
	DONE
)

type MapTask struct {
	id     int
	file   string //入力のファイル名
	status TaskStatus
}

type ReduceTask struct {
	id     int
	files  []string
	status TaskStatus
}

type Coordinator struct {
	mtx          sync.Mutex
	nReduce      int
	mapTasks     []MapTask
	mapRemain    int  //割り振っていないmapタスク
	mapDone      bool //map phaseが完了したか
	reduceTasks  []ReduceTask
	reduceRemain int //割り振っていないreduceタスク
	reduceDone   bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MRTask, reply *MRTask) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	//前回workerが行ったタスクをdoneにする
	if args.Type == MAP {
		c.mapTasks[args.Id].status = DONE
	} else if args.Type == REDUCE {
		c.reduceTasks[args.Id].status = DONE
	}

	fmt.Println(c.mapRemain)
	fmt.Println(c.mapDone)
	fmt.Println(c.IsMapComplete())
	if c.mapRemain > 0 {
		task := c.chooseMapTask()

		reply.Type = MAP
		reply.File = task.file
		reply.Id = task.id
		reply.NReduce = c.nReduce

		c.mapTasks[reply.Id].status = EXECUTING
		c.mapRemain--
	} else if c.mapRemain == 0 && c.reduceRemain > 0 {

		//mapタスクがすべて終わるまでreduceを始めない
		if c.mapDone = c.mapDone || c.IsMapComplete(); !c.mapDone {
			reply.Type = SLEEP
			return nil
		}

		task := c.chooseReduceTask()

		reply.Type = REDUCE
		reply.Intermediates = task.files
		reply.Id = task.id

		c.reduceTasks[reply.Id].status = EXECUTING
		c.reduceRemain--
	} else if c.mapRemain == 0 && c.reduceRemain == 0 {
		//reduceタスクが全て完了しているか
		if !c.IsReduceComplete() {
			reply.Type = SLEEP
			return nil
		}
		c.reduceDone = true
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("start serving")
	go http.Serve(l, nil)
}

// pre: c.mapRemain > 0
func (c *Coordinator) chooseMapTask() MapTask {
	for _, task := range c.mapTasks {
		if task.status == UNDONE {
			return task
		}
	}
	return MapTask{}
}

// pre: c.reduceRemain > 0
func (c *Coordinator) chooseReduceTask() ReduceTask {
	for _, task := range c.reduceTasks {
		if task.status == UNDONE {
			return task
		}
	}
	return ReduceTask{}
}

func (c *Coordinator) IsMapComplete() bool {
	isComplete := true
	for _, task := range c.mapTasks {
		isComplete = isComplete && task.status == DONE
	}
	return isComplete
}

func (c *Coordinator) IsReduceComplete() bool {
	isComplete := true
	for _, task := range c.reduceTasks {
		isComplete = isComplete && task.status == DONE
	}
	return isComplete
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.mapDone && c.reduceDone

	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(inputFiles []string, n int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nReduce:      n,
		mapTasks:     make([]MapTask, len(inputFiles)),
		mapRemain:    len(inputFiles),
		mapDone:      false,
		reduceTasks:  make([]ReduceTask, n),
		reduceRemain: n,
		reduceDone:   false,
	}

	for i, f := range inputFiles {
		c.mapTasks[i] = MapTask{id: i, file: f, status: UNDONE}
	}

	for r := 0; r < n; r++ {
		fs := make([]string, 0, len(inputFiles))
		for m := range inputFiles {
			fileName := fmt.Sprintf("mr-%d-%d", m, r)
			fs = append(fs, fileName)
		}
		c.reduceTasks[r] = ReduceTask{id: r, files: fs, status: UNDONE}
	}

	c.server()
	return &c
}
