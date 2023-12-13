package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	taskId int // remember taskId
)

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	taskId = 9999

	for {
		time.Sleep(time.Second) // Sleep for 1 second
		reply := CallAssign()

		if reply.TaskId < 0 {
			continue
		}

		taskId = reply.TaskId

		if reply.TaskType == "map" {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("Cannot open %v file", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Cannot read %v file", reply.FileName)
			}
			file.Close()
			keyValueArray := mapf(reply.FileName, string(content))

			// fmt.Println(keyValueArray)
			// This contains something like this
			// {indeed 1} {hateful 1} {to 1} {me 1} {and 1} {it 1} {was 1} {during 1} {sleep 1}

			// Store intermediate keyValueArray in tempFile
			tempFileName := "tmp-" + reply.TaskType + "-" + strconv.Itoa(reply.TaskId)

			file, err = os.Create(tempFileName)
			if err != nil {
				log.Fatalf("Cannot create temp file %v", tempFileName)
			} else {
				// fmt.Println("Storing in tempFile ---> ", tempFileName)
			}

			// Transform key, value as json
			enc := json.NewEncoder(file)
			for _, kv := range keyValueArray {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal(err)
				}
			}
			file.Close()
			CallCompleteTask(reply, tempFileName)
		} else if reply.TaskType == "reduce" {

			keyValueArr := []KeyValue{}

			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatal(err)
			}

			dec := json.NewDecoder(file)
			for {
				var keyValue KeyValue
				if err := dec.Decode(&keyValue); err != nil {
					break
				}
				keyValueArr = append(keyValueArr, keyValue)
			}

			reducePhaseOutputFileName := "mr-out-final-" + strconv.Itoa(reply.TaskIndex)
			reducePhaseOutputFile, _ := os.Create(reducePhaseOutputFileName)

			i := 0
			for i < len(keyValueArr) {
				j := i + 1
				for j < len(keyValueArr) && keyValueArr[i].Key == keyValueArr[j].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, keyValueArr[k].Value)
				}

				output := reducef(keyValueArr[i].Key, values)

				fmt.Fprintf(reducePhaseOutputFile, "%v %v\n", keyValueArr[i].Key, output)

				i = j
			}

			reducePhaseOutputFile.Close()

			fmt.Printf("Reduce task %v has finished. \n", reply.TaskIndex)

			CallCompleteTask(reply, reducePhaseOutputFileName)
		} else if reply.TaskType == "close" {
			fmt.Print("Map Reduce completed, Exiting....")
			break
		} else {
			fmt.Println("Unexpected TaskType")
		}

	}

}

func CallCompleteTask(reply MapAssignRequestReply, tempFileName string) {
	args := TaskCompletedRequest{}
	args.FileName = tempFileName
	args.TaskId = reply.TaskId
	args.TaskIndex = reply.TaskIndex
	args.TaskType = reply.TaskType

	completedRequestReply := TaskCompletedRequestReply{}

	call("Coordinator.CompleteTask", &args, &completedRequestReply)
	// fmt.Println(completedRequestReply)
}

func CallAssign() MapAssignRequestReply {
	args := MapAssignRequest{}
	args.TASKID = taskId

	reply := MapAssignRequestReply{}

	call("Coordinator.AssignTask", &args, &reply)
	// fmt.Println(reply)
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
		return true
	}

	fmt.Println(err)
	return false
}
