package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	Start  time.Time
}

type ReduceTask struct {
	id     int
	files  []string
	status TaskStatus
	Start  time.Time
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

	//前回workerが行ったタスクをdoneにする
	if args.Type == MAP {

		//backupで複数のworkerに同じタスクをさせた場合は初回のみ完了処理をする
		if c.mapTasks[args.Id].status == EXECUTING {
			c.mapTasks[args.Id].status = DONE
			c.mapRemain--

			//reduce対象のファイルをセット
			for _, filename := range args.Intermediates {
				reduceId, err := strconv.Atoi(strings.Split(filename, "-")[2]) //assume "mr-{mapid}-{reduceid}"
				if err != nil {
					fmt.Println("Error during conversion")
					return nil
				}
				c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, filename)
			}
		}
	} else if args.Type == REDUCE {
		if c.reduceTasks[args.Id].status == EXECUTING {
			c.reduceTasks[args.Id].status = DONE
			c.reduceRemain--
		}
	}

	if c.mapRemain > 0 { //map phase
		for _, task := range c.mapTasks {
			due := task.Start.Add(10 * time.Second)
			//未着手または時間切れのタスク
			if task.status == UNDONE || (task.status == EXECUTING && time.Now().After(due)) {
				reply.Type = MAP
				reply.File = task.file
				reply.Id = task.id
				reply.NReduce = c.nReduce

				c.mapTasks[task.id].Start = time.Now()
				c.mapTasks[task.id].status = EXECUTING
				return nil
			}
		}
		//割り振れるタスクがない
		reply.Type = SLEEP
	} else if c.mapRemain == 0 && c.reduceRemain > 0 { //reduce phase
		for _, task := range c.reduceTasks {
			due := task.Start.Add(10 * time.Second)
			if task.status == UNDONE || (task.status == EXECUTING && time.Now().After(due)) {
				reply.Type = REDUCE
				reply.Intermediates = task.files
				reply.Id = task.id

				c.reduceTasks[task.id].Start = time.Now()
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
		c.reduceTasks[r] = ReduceTask{id: r, status: UNDONE}
	}

	c.server()
	return &c
}
