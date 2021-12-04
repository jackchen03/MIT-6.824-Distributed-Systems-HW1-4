package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	// Your definitions here.
	stage string
	NumMap, NumReduce int
	remainingTasks map[int]Task
	availableTasks chan Task

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) AssignTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {

	// need to change stage in this phase since if you change stage at the GetReport function,
	// workers haven't received the reply and thus haven't renamed the tmp file the correct filename
	c.mutex.Lock()
	if len(c.remainingTasks) == 0 {
		c.ChangeStage()
	}
	c.mutex.Unlock()
	task, ok := <- c.availableTasks

	if ok {  // there is available task
		// filling reply fields
		c.mutex.Lock()
		defer c.mutex.Unlock()
		reply.TaskType = task.TaskType
		reply.TaskID = task.TaskID
		reply.MapInputFile = task.MapInputFile
		reply.NumMap = c.NumMap
		reply.NumReduce = c.NumReduce
		// handling records in coordinator
		task.WorkerID = args.WorkerID
		task.Deadline = time.Now().Add(10 * time.Second)
		c.remainingTasks[task.TaskID] = task
		//log.Printf("Task type %s, id %d assigned to worker %s", task.TaskType, task.TaskID, task.WorkerID)
	}
	return nil
}

func (c* Coordinator) GetReport(args *ReportTaskArgs, reply *ReportTaskReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.WorkerID != c.remainingTasks[args.TaskID].WorkerID {
		reply.OwnsTheTask = false
		return nil
	}
	reply.OwnsTheTask = true
	delete(c.remainingTasks, args.TaskID)
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
	go http.Serve(l, nil)
}

// Self-defined functions
func (c *Coordinator) ChangeStage() {
	if c.stage == MAP {
		files, _ := ioutil.ReadDir("./")
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "mr-") {
				s := strings.Split(f.Name(), "-")
				i, _ := strconv.Atoi(s[2])
				_, ok := c.remainingTasks[i]
				if !ok{
					task := Task{
						TaskType: REDUCE,
						TaskID:   i,
					}
					c.remainingTasks[i] = task
					c.availableTasks <- task
				}
			}
		}
		c.stage = REDUCE
	} else if c.stage == REDUCE {
		c.stage = END
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	stage := c.stage
	return stage == END
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NumReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, NumReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NumMap = len(files)
	c.NumReduce = NumReduce
	c.stage = MAP

	c.remainingTasks = make(map[int]Task)
	c.availableTasks = make(chan Task, int(math.Max(float64(c.NumMap), float64(c.NumReduce))))

	// preparing the tasks for MAP
	for i, f := range files {
		task := Task{
			TaskType: MAP,
			TaskID: i,
			MapInputFile: f,
		}
		c.remainingTasks[i] = task
		c.availableTasks <- task
	}

	c.server()
	fmt.Println("Coordinator successfully initialized.")

	// collect the tasks of unresponsive workers
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mutex.Lock()
			for TaskID, task := range c.remainingTasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					task.WorkerID = ""
					//log.Printf(
					//	"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
					//	task.TaskType, task.TaskID, task.WorkerID)
					c.availableTasks <- task
					c.remainingTasks[TaskID] = task
				}
			}
			c.mutex.Unlock()
		}
	}()
	return &c
}
