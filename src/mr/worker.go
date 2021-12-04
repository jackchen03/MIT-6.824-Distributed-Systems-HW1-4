package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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
// use ihash(key) % NumReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// get WorkerID from process ID
	id := strconv.Itoa(os.Getpid())
	//log.Printf("Worker %s started\n", id)

	for {
		// if ok == false, means cannot reach coordinator -> EXIT
		applyTaskReply, _ := AskForTask(id)

		if applyTaskReply.TaskType == "" {
			log.Println("Currently no available tasks")
			continue
		}

		//log.Printf("Successfully get task type %s, task id %d, my workerID is %s", applyTaskReply.TaskType, applyTaskReply.TaskID, id)

		// start doing map or reduce
		if applyTaskReply.TaskType == MAP {
			// read content from files
			filename := applyTaskReply.MapInputFile
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// get key-value pair array from the map function
			kva := mapf(filename, string(content))

			// put key-value pairs under different buckets
			var buckets map[int] []KeyValue
			buckets = make(map[int] []KeyValue)
			for _, kv := range kva {
				reduceIdx := ihash(kv.Key) % applyTaskReply.NumReduce
				buckets[reduceIdx] = append(buckets[reduceIdx], kv)
			}

			// write to temp output files
			mapIdx := applyTaskReply.TaskID
			tmpFileNames := make(map[string] string)
			for reduceIdx, kva := range buckets{
				tmpName := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
				mapOutputFile, err := ioutil.TempFile("./", tmpName)
				tmpFileNames[tmpName] = mapOutputFile.Name()
				if err != nil {
					log.Fatalf("cannot open %v", tmpName)
				}
				for _, kv := range kva {
					enc := json.NewEncoder(mapOutputFile)
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Encode fail on key %s and value %s", kv.Key, kv.Value)
					}
				}
				mapOutputFile.Close()
			}

			// report to coordinator
			reportTaskReply, _ := ReportTask(applyTaskReply.TaskType, applyTaskReply.TaskID, id)
			if reportTaskReply.OwnsTheTask { // write the temporary file
				for reduceIdx, _ := range buckets{
					filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
					os.Rename(tmpFileNames[filename], filename)
				}
			}
		} else {  // reduce
			reduceIdx := applyTaskReply.TaskID
			var intermediate []KeyValue
			// for every mapIdx with the same reduceIdx, put the intermediate files together
			for mapIdx := 0;mapIdx<applyTaskReply.NumMap;mapIdx++ {
				filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
				file, err := os.Open(filename)
				if err != nil {
					// in some cases the file might not exist since not all mapID appears in every reduce task
					// log.Printf("cannot open %v", filename)
					continue
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			// write to temp file
			tmpname := fmt.Sprintf("mr-out-%d-%s", reduceIdx, id)
			ofile, _ := ioutil.TempFile("./", tmpname)
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
				s := fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
				ofile.WriteString(s)

				i = j
			}

			// report to coordinator
			reportTaskReply, _ := ReportTask(applyTaskReply.TaskType, applyTaskReply.TaskID, id)
			if reportTaskReply.OwnsTheTask {  // write the temporary file
				os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceIdx))
			}
			ofile.Close()
		}
		// wait some time before shooting another request
		time.Sleep(200*time.Millisecond)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Coordinator.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

func AskForTask(WorkerID string) (ApplyTaskReply, bool){
	args := ApplyTaskArgs{WorkerID}
	reply := ApplyTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	return reply, ok
}

func ReportTask(TaskType string, TaskID int, WorkerID string) (ReportTaskReply, bool){
	args := ReportTaskArgs{TaskType, TaskID, WorkerID}
	reply := ReportTaskReply{}

	ok := call("Coordinator.GetReport", &args, &reply)
	return reply, ok
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

	log.Fatal(err)
	return false
}
