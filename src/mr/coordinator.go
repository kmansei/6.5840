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
	id      int
	file    string //入力のファイル名
	done    bool
	nReduce int
}

type Coordinator struct {
	mtx      sync.Mutex
	mapTasks []MapTask
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
		reply.NReduce = task.nReduce
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
	ret := len(c.mapTasks) == 0

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(inputFiles []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: make([]MapTask, len(inputFiles)),
	}

	// Your code here.
	for i, f := range inputFiles {
		c.mapTasks[i] = MapTask{id: i, file: f, done: false, nReduce: 10}
	}

	c.server()
	return &c
}
