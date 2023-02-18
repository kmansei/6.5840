package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
	args := MRTask{Type: INIT}

	for {
		reply := CallGetTask(&args)

		if reply.Type == MAP {
			fmt.Println(reply.File)

			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v for map task", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v for map task", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))

			//nReduce個に分散された中間ファイル
			intermediate := make(map[int][]KeyValue)
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.NReduce
				intermediate[i] = append(intermediate[i], kv)
			}

			for reduceid, kvarr := range intermediate {
				fname := fmt.Sprintf("mr-%d-%d", reply.Id, reduceid)

				ofile, _ := os.Create(fname)
				enc := json.NewEncoder(ofile)

				for _, kv := range kvarr {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				ofile.Close()
			}
			args = MRTask{Type: MAP, Id: reply.Id}
		} else if reply.Type == REDUCE {
			fmt.Printf("Reduce Id: %d\n", reply.Id)
			fmt.Println(reply.Intermediates)

			intermediate := []KeyValue{}
			for _, f := range reply.Intermediates {
				fmt.Println(f)
				file, err := os.Open(f)

				if err != nil {
					log.Fatalf("cannot open %v for reduce task", f)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.Id)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			args = MRTask{Type: REDUCE, Id: reply.Id}
		} else if reply.Type == SLEEP {
			fmt.Println("sleeping...")
			time.Sleep(500 * time.Millisecond)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallGetTask(args *MRTask) MRTask {
	reply := MRTask{}

	ok := call("Coordinator.GetTask", args, &reply)
	if !ok {
		fmt.Printf("call Coordinator.GetTask failed!\n")
		os.Exit(0)
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		fmt.Println("err == nil")
		return true
	}
	fmt.Println("err != nil")

	fmt.Println(err)
	return false
}
