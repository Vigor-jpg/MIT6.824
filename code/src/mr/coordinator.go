package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const IDLE int = 2
const SEND int = 1
const COMPETE int = 0

const Map int = 0
const Reduce int = 1
const Empty int = 2
const FinishAll = 3

const ReduceDir string = "mr/"
const MapDir string = "main/"

type Task struct {
	Type     int
	FileName string
	Status   int
	Time     time.Time
}
type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex

	nMap    int
	nReduce int

	nMapSend    int
	nReduceSend int

	nMapFinished    int
	nReduceFinished int

	maps        []Task
	mapFinished bool

	reduces        []Task
	reduceFinished bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) MapTaskAssign(task *TaskAssignReply){
	if c.nMapSend < c.nMap {
		*task = TaskAssignReply{
			FileName:  c.maps[c.nMapSend].FileName,
			TaskIndex: c.nMapSend,
			Type:      Map,
		}
		c.maps[c.nMapSend].Time = time.Now()
		c.maps[c.nMapSend].Status = SEND
		c.nMapSend++
	} else {
		for i, t := range c.maps {
			if t.Status == SEND {
				usedTime := time.Since(t.Time)
				if usedTime > 10*time.Second {
					*task = TaskAssignReply{
						FileName:  task.FileName,
						TaskIndex: i,
						Type:      Map,
					}
					t.Time = time.Now()
					return
				}
			}
		}
		(*task).Type = Empty
	}
}
func (c *Coordinator) ReduceTaskAssign(task *TaskAssignReply) {
	if c.nReduceSend < c.nReduce {
		*task = TaskAssignReply{
			FileName:  c.maps[c.nReduceSend].FileName,
			TaskIndex: c.nReduceSend,
			Type:      Reduce,
			nReduce:   c.nReduce,
			nMaps:     c.nMap,
		}
		c.reduces[c.nReduceSend].Time = time.Now()
		c.reduces[c.nReduceSend].Status = SEND
		c.nReduceSend++
	} else {
		for i, t := range c.reduces {
			if t.Status == SEND {
				usedTime := time.Since(t.Time)
				if usedTime > 10*time.Second {
					*task = TaskAssignReply{
						FileName:  task.FileName,
						TaskIndex: i,
						Type:      Reduce,
						nReduce:   c.nReduce,
						nMaps:     c.nMap,
					}
					t.Time = time.Now()
					return
				}
			}
		}
		(*task).Type = Empty
	}
}
func (c *Coordinator) TaskAssign(args *TaskAssignArgs,task *TaskAssignReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.mapFinished {
		c.MapTaskAssign(task)
	} else if !c.reduceFinished {
		c.ReduceTaskAssign(task)
	} else {
		task.Type = FinishAll
	}
	return nil
}

func (c *Coordinator) MapCompete(args *MapCompeteArgs,reply MapCompeteReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.maps[args.TaskIndex].Status = COMPETE
	c.nMapFinished++
	if c.nMapFinished == c.nMap {
		c.mapFinished = true
	}
	return nil
}

func (c *Coordinator) ReduceCompete(args *ReduceCompeteArgs,reply ReduceCompeteReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.reduces[args.TaskIndex].Status = COMPETE
	c.nMapFinished++
	if c.nReduceFinished == c.nReduce {
		c.reduceFinished = true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go func(){
		_ = http.Serve(l,nil)
		fmt.Println("Server has been start")
	}()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.mapFinished && c.reduceFinished {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	len := len(files)
	c := Coordinator{
		nReduce:         nReduce,
		nMap:            len,
		nMapFinished:    0,
		nReduceFinished: 0,
		maps:            make([]Task, len),
		reduces:         make([]Task, nReduce),
		reduceFinished:  false,
		mapFinished:     false,
	}
	// Your code here
	for i, file := range files {
		c.maps[i] = Task{
			FileName: file,
			Status:   IDLE,
			Type:     Map,
		}
	}
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("%s%d%s", "mr-", i, ".txt")
		_, err := os.Create(fileName)
		if err != nil {
			panic("创建文件失败")
		}
		c.reduces[i] = Task{
			FileName: fileName,
			Status:   IDLE,
			Type:     Reduce,
		}
	}
	c.server()
	return &c
}
