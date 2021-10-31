package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func DoMap(mapf func(string, string) []KeyValue, task TaskAssignReply) {
	content := readFile(task.FileName)
	kva := mapf(task.FileName, content)
	maps := make(map[string]int, 0)
	for _, kv := range kva {
		num, ok := maps[kv.Key]
		if ok {
			maps[kv.Key] = num + 1
		} else {
			maps[kv.Key] = 1
		}
	}
	write := make([]string, task.nReduce)
	for k, v := range maps {
		str := fmt.Sprintf("%s:%d,", k, v)
		hash := ihash(k) % task.nReduce
		write[hash] += str
	}
	for i, str := range write {
		fileName := fmt.Sprintf("%s/mr-%d-%d.txt", ReduceDir, task.TaskIndex, i)
		file, err2 := os.Create(fileName)
		if err2 != nil {
			panic(err2)
		}
		_, err3 := file.WriteString(str)
		if err3 != nil {
			panic(err3)
		}
	}
	args := MapCompeteArgs{
		TaskIndex: task.TaskIndex,
	}
	reply := MapCompeteReply{}
	call("Coordinator.MapCompete", &args, &reply)
}
func DoReduce(reply TaskAssignReply) {
	var content string
	for i := 0; i < reply.nMaps; i++ {
		fileName := fmt.Sprintf("%s/mr-%d-%d.txt", ReduceDir, i, reply.TaskIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		str, err1 := ioutil.ReadAll(file)
		if err1 != nil {
			panic(err1)
		}
		content += string(str)
	}
	fileName := fmt.Sprintf("%s/mr-out-%d", MapDir, reply.TaskIndex)
	file, err4 := os.Create(fileName)
	if err4 != nil {
		panic(err4)
	}
	kva := StringParse(content)
	for _, kv := range kva {
		str := kv.Key + " " + kv.Value + "\n"
		_, err5 := file.WriteString(str)
		if err5 != nil {
			panic(err5)
		}
	}
	args := ReduceCompeteArgs{
		TaskIndex: reply.TaskIndex,
	}
	rep := ReduceCompeteReply{}
	call("Coordinator.ReduceCompete", &args, &rep)
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := TaskAssignArgs{}
		reply := TaskAssignReply{}
		call("Coordinator.TaskAssign", &args, &reply)
		if reply.Type == Map {
			DoMap(mapf, reply)
		} else if reply.Type == Reduce {
			DoReduce(reply)
		} else if reply.Type == Empty {
			time.Sleep(time.Second)
		} else {
			fmt.Sprintf("all task has been done,pid %d is over", os.Getpid())
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}
	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
		return true
	}

	fmt.Println(err)
	return false
}
