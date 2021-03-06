package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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
	fmt.Println("domap has been start!")
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
	write := make([]string, task.NReduce)
	for _, kv := range kva {
		str := fmt.Sprintf("%s:%s,", kv.Key, kv.Value)
		hash := ihash(kv.Key) % task.NReduce
		write[hash] += str
	}
	//fmt.Println("domap: map over")
	for i, str := range write {
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskIndex, i)
		file, err2 := os.Create(fileName)
		if err2 != nil {
			panic(err2)
		}
		_, err3 := file.WriteString(str)
		if err3 != nil {
			panic(err3)
		}
	}
	//fmt.Println("domap: writefile over")
	args := MapCompeteArgs{
		TaskIndex: task.TaskIndex,
	}
	reply := MapCompeteReply{}
	call("Coordinator.MapCompete", &args, &reply)
}
func DoReduce(reducef func(string, []string) string,reply TaskAssignReply) {
	var content string
	for i := 0; i < reply.NMaps; i++ {
		fileName := fmt.Sprintf("mr-%d-%d",i, reply.TaskIndex)
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
	//fmt.Println("doreduce read over")
	fileName := fmt.Sprintf("mr-out-%d", reply.TaskIndex)
	file, err4 := os.Create(fileName)
	if err4 != nil {
		panic(err4)
	}
	//fmt.Println("file creat over")
	kva := StringParse(content[0:len(content)-1])
	//fmt.Println("StringParse Over")
	kvb := make(map[string][]string)
	for _, kv := range kva {
		str,ok := kvb[kv.Key]
		if ok{
			kvb[kv.Key] = append(str,kv.Value)
		}else{
			list := make([]string,1)
			list[0] = kv.Value
			kvb[kv.Key] = list
		}
	}
	kva = make([]KeyValue,0)
	for k,v := range kvb{
		val := reducef(k,v)
		//str := fmt.Sprintf("%s %s",k,val)
		kva = append(kva,KeyValue{k,val})
	}
	sort.SliceStable(kva, func(i,j int) bool {
		return strings.Compare(kva[j].Key,kva[i].Key) == 1
	})
	for _,kv := range kva{
		str := fmt.Sprintf("%s %s\n",kv.Key,kv.Value)
		_,err4 = file.WriteString(str)
		if err4 != nil{
			log.Fatal(err4)
		}
	}
	//fmt.Println("file write over")
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
			DoReduce(reducef,reply)
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
