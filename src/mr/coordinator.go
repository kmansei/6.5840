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

type MapTask struct {
	id   int
	file string //入力のファイル名
}

type ReduceTask struct {
	id    int
	files []string
}

type Coordinator struct {
	mtx         sync.Mutex
	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *ExampleArgs, reply *MRTask) error {
	c.mtx.Lock()
	fmt.Println("-------------")
	fmt.Printf("c.mapTask: %d\n", len(c.mapTasks))
	if len(c.mapTasks) > 0 {

		//mapTasksをdeque
		task := c.mapTasks[0]
		c.mapTasks = c.mapTasks[1:]

		reply.Type = Map
		reply.File = task.file
		reply.Id = task.id
		reply.NReduce = c.nReduce
	} else if len(c.mapTasks) == 0 && len(c.reduceTasks) > 0 {
		task := c.reduceTasks[0]
		c.reduceTasks = c.reduceTasks[1:]

		reply.Type = Reduce
		reply.Intermediates = task.files

		reply.Id = task.id
		reply.NReduce = c.nReduce
	}
	c.mtx.Unlock()
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := len(c.mapTasks) == 0 && len(c.reduceTasks) == 0

	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(inputFiles []string, n int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]MapTask, len(inputFiles)),
		reduceTasks: make([]ReduceTask, n),
	}

	// Your code here.
	c.nReduce = n

	for i, f := range inputFiles {
		c.mapTasks[i] = MapTask{id: i, file: f}
	}

	for r := 0; r < n; r++ {
		fs := make([]string, 0, len(inputFiles))
		for m := range inputFiles {
			fileName := fmt.Sprintf("mr-%d-%d", m, r)
			fs = append(fs, fileName)
		}
		c.reduceTasks[r] = ReduceTask{id: r, files: fs}
	}

	c.server()
	return &c
}
