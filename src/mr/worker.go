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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type MyWorker struct {
	Task    Task
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		worker := MyWorker{mapf: mapf, reducef: reducef}
		worker.CallAskReply()
		switch worker.Task.TaskState {
		case RUN:
			if worker.Task.State == MAP {
				worker.doMapWorker()
				worker.CallTaskOver(&worker.Task)
			} else {
				worker.doReduceWorker()
				worker.CallTaskOver(&worker.Task)
			}
		case WAIT:
			time.Sleep(time.Millisecond * 500)
		case DONE:
			return
		}
		
	}

}
func getMapFileName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%v-%v", mapId, reduceId)
}
func (w *MyWorker) doMapWorker() {
	// fmt.Println("map begin")
	readFile, err := ioutil.ReadFile(w.Task.FileName)
	if err != nil {
		fmt.Printf("Cannot open %v", w.Task.FileName)
		return
	}
	kv := w.mapf(w.Task.FileName, string(readFile)) //键值对
	reduceRes := make([][]KeyValue, w.Task.NReduce) // w.task.NReduce是数组的第一维的大小
	for _, value := range kv {//分配
		idx := ihash(value.Key) % w.Task.NReduce
		reduceRes[idx] = append(reduceRes[idx], value)
	}
	for i, kvs := range reduceRes { //i就是Rid

		fileName := getMapFileName(w.Task.Mid, i)
		tempFile, err := ioutil.TempFile("", fileName)
		defer os.Remove(tempFile.Name())
		if err != nil {
			return
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		os.Rename(tempFile.Name(), fileName)
	}
	w.Task.TaskState = DONE
}
func getReduceFileName(reduceId int) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}
func (w *MyWorker) doReduceWorker() {
//	fmt.Printf("Reduce Action%v\n",w.Task.Rid)
	kva := make(map[string][]string) //key 是string，value是[]string（string数组）
	var ks []string
	for i := 0; i < w.Task.NMap; i++ {
		mapFileName := getMapFileName(i, w.Task.Rid)
		readFile, err := os.Open(mapFileName)
		if err != nil {
			return
		}
		dec := json.NewDecoder(readFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := kva[kv.Key]; !ok {
				ks = append(ks, kv.Key)
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}
	sort.Strings(ks)
	fileName := getReduceFileName(w.Task.Rid)
	tempFile, err := ioutil.TempFile("", fileName)//在mr-tmp目录下创建临时文件
	defer os.Remove(tempFile.Name())
	for _, key := range ks {
		res := w.reducef(key, kva[key])//（key,list）->(key,value)
		if err != nil {
			return
		}
		_, err = tempFile.WriteString(fmt.Sprintf("%v %v\n", key, res))
		if err != nil {
			return
		}
		os.Rename(tempFile.Name(), fileName)
		w.Task.TaskState = DONE
	}
}
func (w *MyWorker) CallTaskOver(args *Task) {

	reply := ExampleReply{}
	call("Coordinator.ReportTask", args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (w *MyWorker) CallAskReply() {
	args := ExampleArgs{}
	reply := AskReply{}
	if !call("Coordinator.AllocateTest", &args, &reply){
		os.Exit(1)
	}
	w.Task = reply.Task
}

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
