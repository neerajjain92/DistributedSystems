package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// For Sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	IDLE              = 0
	INPROGRESS        = 1
	COMPLETED         = 2
	WAIT_TIME_IN_SECS = 10 // wait time in seconds for a task to be finished
)

type MapTask struct {
	filename string // filename for map operation
	mapId    int    // id of the map task
	state    int    // idle, in-progress, completed
}

type ReduceTask struct {
	filename string // intermediate filename for reduce operation
	reduceId int    // id of the reduce task
	state    int    // idle, in-progress, completed
}

type Coordinator struct {
	mapTasks             []MapTask
	reduceTasks          []ReduceTask
	mapPhaseCompleted    bool
	reducePhaseCompleted bool
}

var (
	mapId                    int
	mapTasksToBeCompleted    int
	reduceTasksToBeCompleted int
	intermediateFiles        []string
	reduceId                 int
	mutex                    sync.Mutex
	mapChanArr               [10]chan int
	reduceChanArr            [10]chan int
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *MapAssignRequest, reply *MapAssignRequestReply) error {
	mutex.Lock()

	if !c.mapPhaseCompleted {
		reply.TaskType = "map"
		reply.TaskId = -1 // Negative means no map phase to be invoked

		for index, task := range c.mapTasks {
			// Find the first idle map task
			if task.state == IDLE {
				c.mapTasks[index].state = INPROGRESS
				c.mapTasks[index].mapId = mapId
				reply.FileName = task.filename
				reply.TaskId = mapId
				reply.TaskIndex = index // To remember which task has done
				mapId++
				go limitMapTimer(c, index, mapChanArr[index])
				break
			}
		}
	} else if !c.reducePhaseCompleted {
		// Map phase completed, now let's simply start reduce tasks
		reply.TaskType = "reduce"
		reply.TaskId = -1

		for index, task := range c.reduceTasks {
			if task.state == IDLE {
				c.reduceTasks[index].state = INPROGRESS
				c.reduceTasks[index].reduceId = reduceId
				reply.FileName = task.filename
				reply.TaskId = reduceId
				reply.TaskIndex = index // Remember which task has completed
				reduceId++
				go limitReduceTimer(c, index, reduceChanArr[index])
				break
			}
		}

		// fmt.Println(c.reduceTasks)

	} else {
		reply.TaskType = "close"
	}

	mutex.Unlock()
	return nil
}

func limitReduceTimer(c *Coordinator, index int, channel chan int) {
	select {
	case x := <-channel:
		if x == index {
			fmt.Printf("Reduce task %v has finished under 10 seconds \n", index)
		}
		mutex.Lock()
		reduceTasksToBeCompleted--
		if reduceTasksToBeCompleted == 0 {
			c.reducePhaseCompleted = true
			fmt.Println("All Reduce Tasks completed")
		}
		c.reduceTasks[index].state = COMPLETED
		mutex.Unlock()

	case <-time.After(time.Second * WAIT_TIME_IN_SECS):
		fmt.Printf("Reduce task %v timedout", index)
		mutex.Lock()
		c.reduceTasks[index].state = IDLE
		// Incrementing to discard the reply came from either original worker
		// when the task is already assigned to another worker
		// See CompleteTask implementation, and you'll know why we did this
		c.reduceTasks[index].reduceId++
		mutex.Unlock()
	}
}

// Limit a task to 10 second, and change state to IDLE if task surpasses this threshold
func limitMapTimer(c *Coordinator, index int, channel chan int) {
	select {
	case x := <-channel:
		// if x == index {
		// 	// fmt.Printf("Map task %v has finished in 10 second \n", index)
		// }
		mutex.Lock()
		mapTasksToBeCompleted--
		if mapTasksToBeCompleted == 0 {
			c.mapPhaseCompleted = true
			fmt.Println("All Map Tasks completed ", x)
			splitIntoNBuckets(c)
		}
		c.mapTasks[index].state = COMPLETED
		mutex.Unlock()

	case <-time.After(time.Second * WAIT_TIME_IN_SECS):
		fmt.Printf("Map task %v timedout", index)
		mutex.Lock()
		c.mapTasks[index].state = IDLE
		// Incrementing to discard the reply came from either original worker
		// when the task is already assigned to another worker
		// See CompleteTask implementation, and you'll know why we did this
		c.mapTasks[index].mapId++
		mutex.Unlock()

	}

}

func splitIntoNBuckets(c *Coordinator) {
	fmt.Printf("All Map tasks completed, it's time to split them into %v buckets \n,", reduceTasksToBeCompleted)
	// fmt.Println(intermediateFiles)

	keyValueArr := []KeyValue{}
	for _, fileName := range intermediateFiles {
		file, err := os.Open(fileName)
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
	}

	sort.Sort(ByKey(keyValueArr))

	totalKeyValuePairs := len(keyValueArr)
	bucketSize := totalKeyValuePairs / reduceTasksToBeCompleted
	if bucketSize < 1 {
		bucketSize = 1
	}

	fmt.Println("Bucket Size: ", bucketSize)

	startPos := 0
	endPos := 0
	index := 0

	for {
		if startPos+bucketSize < totalKeyValuePairs {
			endPos = startPos + bucketSize

			// Assign same keys to the same reduceTask
			for endPos < totalKeyValuePairs-1 && keyValueArr[endPos].Key == keyValueArr[endPos+1].Key {
				endPos++
			}
		} else {
			endPos = totalKeyValuePairs - 1
		}
		endPos++
		// fmt.Printf("StartPos %d, EndPos %d", startPos, endPos)

		// Now create a tempFile where these splitted tasks to be stored
		// so that reduce task can use them
		tempFileName := "mr-tmp-" + strconv.Itoa(index)

		file, err := os.Create(tempFileName)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(file)
		for _, kv := range keyValueArr[startPos:endPos] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		file.Close()

		// fmt.Printf("TmpFile %v \n", tempFileName)
		c.reduceTasks[index].filename = tempFileName
		c.reduceTasks[index].state = IDLE
		c.reduceTasks[index].reduceId = reduceId
		startPos = endPos
		index++
		if index == reduceTasksToBeCompleted {
			break
		}
	}

	// fmt.Println(c.reduceTasks)
}

func (c *Coordinator) CompleteTask(args *TaskCompletedRequest, reply *TaskCompletedRequestReply) error {
	reply.Message = "Yor should be faster, keep going... Hurray...."

	// Map task completed
	if args.TaskType == "map" {

		// As you know during both map and reduce phase the taskId was
		// always assigned as mapId variable defined above, Now keeping that in mind read below
		// if taskId is less than the mapTasks[index].mapId, it means that this task has been assigned to another
		// worker since this worker couldn't complete the task in time (which is 10second threshold)
		if args.TaskId >= c.mapTasks[args.TaskIndex].mapId {
			mapChanArr[args.TaskIndex] <- args.TaskIndex // Tell the timer to stop
			intermediateFiles = append(intermediateFiles, args.FileName)
			reply.Message = "You finished [" + strconv.Itoa(args.TaskId) + "] Map task, Keep going"
		}
	} else if args.TaskType == "reduce" {
		if args.TaskId >= c.reduceTasks[args.TaskIndex].reduceId {
			reduceChanArr[args.TaskIndex] <- args.TaskIndex
		}
	}
	// fmt.Println(intermediateFiles)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.mapPhaseCompleted && c.reducePhaseCompleted {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Initializing coordinator state
	c.mapTasks = []MapTask{}
	c.reduceTasks = make([]ReduceTask, nReduce)
	c.mapPhaseCompleted = false
	c.reducePhaseCompleted = false

	// Initialize channel
	for i := range mapChanArr {
		mapChanArr[i] = make(chan int, 1)
	}
	for i := range reduceChanArr {
		reduceChanArr[i] = make(chan int, 1)
	}

	// Start the Server
	go c.server()

	mapTasksToBeCompleted = len(files)
	reduceTasksToBeCompleted = nReduce

	for _, fileName := range files {
		// Initializing all mapTasks with mapId as 1, since we will be incrementing
		// the mapId variable declared above when worker comes and ask for a Tasks from the coordinator
		mapTask := MapTask{filename: fileName, mapId: 1, state: IDLE}
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	fmt.Println(c.mapTasks)

	return &c
}
