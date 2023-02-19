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
	mapRemain    int //完了していないmapタスク
	reduceTasks  []ReduceTask
	reduceRemain int //完了していないreduceタスク
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MRTask, reply *MRTask) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	//fmt.Println(c.reduceTasks)

	//前回workerが行ったタスクをdoneにする
	if args.Type == MAP {
		c.mapTasks[args.Id].status = DONE
		c.mapRemain--
	} else if args.Type == REDUCE {
		c.reduceTasks[args.Id].status = DONE
		c.reduceRemain--
	}

	if c.mapRemain > 0 { //map phase
		for _, task := range c.mapTasks {
			if task.status == UNDONE {
				reply.Type = MAP
				reply.File = task.file
				reply.Id = task.id
				reply.NReduce = c.nReduce
				c.mapTasks[task.id].status = EXECUTING
				return nil
			}
		}
		//割り振れるタスクがない
		reply.Type = SLEEP
	} else if c.mapRemain == 0 && c.reduceRemain > 0 { //reduce phase
		for _, task := range c.reduceTasks {
			if task.status == UNDONE {
				reply.Type = REDUCE
				reply.Intermediates = task.files
				reply.Id = task.id
				c.reduceTasks[task.id].status = EXECUTING
				return nil
			}
		}
		//割り振れるタスクがない
		reply.Type = SLEEP
	} else { //map phaseとreduce phaseが完了
		reply.Type = EXIT
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
	go http.Serve(l, nil)
}

// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapRemain == 0 && c.reduceRemain == 0
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
		reduceTasks:  make([]ReduceTask, n),
		reduceRemain: n,
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
