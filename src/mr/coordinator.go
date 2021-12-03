package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP    int = 1
	REDUCE int = 2
	WAIT   int = 3
	RUN    int = 4
	DONE   int = 5
)
const (
	TaskMaxRunTime = time.Second * 10
	CollectInterval = time.Millisecond * 500
)

type Coordinator struct {
	// Your definitions here.
	MapWaitQueue    chan Task
	ReduceWaitQueue chan Task
	MapRunQueue     chan Task
	ReduceRunQueue  chan Task
	TaskState       int //RUN//DONE
	NMap            int
	NReduce         int
	mu              sync.Mutex
}
type Task struct {
	Mid       int
	Rid       int
	FileName  string
	BeginTime time.Time
	State     int //MAP ,REDUCR
	TaskState int //WAIT||RUN||DONE
	NMap      int
	NReduce   int
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
func (c *Coordinator) distribute() {
	
	reduceTask := Task{
		Mid:       0,
		State:     REDUCE,
		TaskState: WAIT,
		NMap:      c.NMap,
		NReduce:   c.NReduce,
	}
	for reduceIndex := 0; reduceIndex < c.NReduce; reduceIndex++ {
		//fmt.Printf("Create %vth reduce task \n",reduceIndex)
		task := reduceTask
		task.Rid = reduceIndex
		c.ReduceWaitQueue <- task
	}
}
func (c *Coordinator) AllocateTest(args *ExampleArgs, reply *AskReply) error {
	lenMapWait :=len(c.MapWaitQueue)
	lenReduceWait :=len(c.ReduceWaitQueue)
	if lenMapWait > 0 {
		reply.Task = <-c.MapWaitQueue
		// fmt.Printf("Allocate %dth Map Task \n",reply.Task.Mid)
		reply.Task.BeginTime = time.Now()
		reply.Task.TaskState = RUN
		c.MapRunQueue <- reply.Task
		return nil
	} else if lenReduceWait > 0 {
		reply.Task = <-c.ReduceWaitQueue
		// fmt.Printf("Allocate %dth Reduce Task \n",reply.Task.Rid)
		reply.Task.BeginTime = time.Now()
		reply.Task.TaskState = RUN
		c.ReduceRunQueue <- reply.Task
		return nil
	}
	
	if len(c.MapRunQueue) > 0 || len(c.ReduceRunQueue) > 0 {
		reply.Task.TaskState = WAIT
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TaskState = DONE
	reply.Task.TaskState = DONE
	return nil
	
}
func (c *Coordinator) RemoveTask(state int, mid int, rid int) {
	
	switch state {
	case REDUCE:
		len:=len(c.ReduceRunQueue)
		for i := 0; i < len; i++ {
			task := <-c.ReduceRunQueue			
			task.TaskState=RUN
			if task.Rid == rid /*&& task.Mid == mid*/ {
				task.TaskState=DONE
				//fmt.Printf("Reduce task on %dth Map  %dth Reduce complete\n", mid, rid)
			} else {
				c.ReduceRunQueue <- task
			}
		}
	case MAP:
		len:=len(c.MapRunQueue)
		for i := 0; i < len; i++ {		
			task := <-c.MapRunQueue
			task.TaskState=RUN
			if task.Mid == mid/*&& task.Mid == mid*/ {
				task.TaskState=DONE
				//fmt.Printf("Map task on %vth Map  %vth Reduce complete\n", mid, rid)
			} else {
				c.MapRunQueue <- task
			}
		}
	}

}
func (c *Coordinator) ReportTask(args *Task, reply *ExampleReply) error {
	//log.Println("report called")
	
	switch args.State {
	case MAP:
		c.RemoveTask(MAP, args.Mid, args.Rid)
		if len(c.MapRunQueue) == 0 && len(c.MapWaitQueue) == 0 {
		//	fmt.Println("distribute Action.......................................")
			c.distribute()
		//	fmt.Println("distribute Over.........................................")
		}
	case REDUCE:
		// fmt.Println("DeleteReduce")
		c.RemoveTask(REDUCE, args.Mid, args.Rid)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	c.mu.Lock()
	defer c.mu.Unlock()
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.TaskState == DONE
}

func (c *Coordinator) collectOutOfTime() {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	for !c.Done() {
		// fmt.Printf("MapNnm%d\n",len(c.MapRunQueue))
		lenmap :=len(c.MapRunQueue);
		for i := 0; i < lenmap; i++ {
			task := Task{}
			task = <-c.MapRunQueue
			//fmt.Printf("%v",task.BeginTime)
			if time.Since(task.BeginTime) > TaskMaxRunTime {
				// fmt.Println("OutOfTime")
				task.TaskState = WAIT
				c.MapWaitQueue <- task
			} else {
				c.MapRunQueue <- task
			}
		}
		// fmt.Printf("Reduce%v",len(c.ReduceRunQueue))
		lenreduce := len(c.ReduceRunQueue)
		for i := 0; i < lenreduce; i++ {
			task := Task{}
			task = <-c.ReduceRunQueue
			//fmt.Printf("Reduce%d",task.BeginTime)
			if time.Since(task.BeginTime) > TaskMaxRunTime {
				// fmt.Println("OutOfTime")
				task.TaskState = WAIT
				c.ReduceWaitQueue <- task
			} else {
				c.ReduceRunQueue <- task
			}
		}
		time.Sleep(CollectInterval)
	}
	// fmt.Println("All Task Done")
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c = Coordinator{
		TaskState:       RUN,
		NMap:            len(files),
		NReduce:         nReduce,
		MapWaitQueue:    make(chan Task, len(files)),
		MapRunQueue:     make(chan Task, len(files)),
		ReduceWaitQueue: make(chan Task, len(files)),
		ReduceRunQueue:  make(chan Task, len(files)),
	}
	if len(files) < nReduce {

		c.MapWaitQueue = make(chan Task, nReduce)
		c.MapRunQueue = make(chan Task, nReduce)
		c.ReduceWaitQueue = make(chan Task, nReduce)
		c.ReduceRunQueue = make(chan Task, nReduce)
	}
	for fileIndex := 0; fileIndex < len(files); fileIndex++ {
		//fmt.Printf("%vth file,%v Waitting\n", fileIndex, files[fileIndex])
		maptask := Task{
			Mid:       fileIndex,
			FileName:  files[fileIndex],
			State:     MAP,
			TaskState: WAIT,
			Rid:       0, //初始化  无意义
			NMap:      len(files),
			NReduce:   nReduce,
		}
		c.MapWaitQueue <- maptask
	}

	go c.collectOutOfTime() //回收超时worker

	c.server()
	return &c
}
