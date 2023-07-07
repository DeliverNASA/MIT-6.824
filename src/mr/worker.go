package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

var NReduce int

const (
	RPC_FAILED_CODE    = -10
	RPC_FAILED_MESSAGE = "rpc failed"
)

//
// main/mrworker.go calls this function.
//

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// 获取reducer数目，便于中间文件的命名
	NReduce = CallGetInfo()
	if NReduce == RPC_FAILED_CODE {
		return
	}

	// Phase: Map
	// 如果map任务还没完成就继续做
	// -2: 进入到reduce了；-1：当前没有任务但是还没进入到reduce；other：有map任务可以做
	for {
		MapID, filename := CallAskForTask("map")
		if MapID == RPC_FAILED_CODE {
			return
		}
		if MapID == -2 {
			fmt.Printf("All map tasks have been finished.\n")
			break
		} else if MapID == -1 {
			fmt.Printf("No map tasks now but waiting for other workers...\n")
			time.Sleep(time.Second)
		} else {
			fmt.Printf("Worker get a MapID %v, dealing with %v\n", MapID, filename)
			MapPhase(mapf, MapID, filename)
			CallFinishTask("map", MapID)
		}
	}

	// Phase: Reduce
	// 等待master发出reduce信号后再开始进行reduce操作
	// -2: 还没进入reduce；-1：当前没有任务但是reduce还没完成；other：有reduce任务可以做
	for {
		// NReduce, ReduceID = CallGetInfo("reduce")
		ReduceID, _ := CallAskForTask("reduce")
		if ReduceID == RPC_FAILED_CODE {
			return
		}
		if ReduceID == -2 {
			fmt.Printf("Reduce phase has not yet started.\n")
			time.Sleep(time.Second)
		} else if ReduceID == -1 {
			fmt.Printf("No reduce tasks now but waiting for other workers...\n")
			time.Sleep(time.Second)
		} else {
			fmt.Printf("Worker get a ReduceID %v\n", ReduceID)
			ReducePhase(reducef, ReduceID)
			CallFinishTask("reduce", ReduceID)
		}
	}

}

func MapPhase(mapf func(string, string) []KeyValue,
	MapID int, filename string) {

	inter_files_set := make([]*os.File, NReduce)
	inter_files_name_set := make([]string, NReduce)
	encoder_set := make([]*json.Encoder, NReduce)

	for i := 0; i < NReduce; i++ {
		// 创建一个临时文件，完成后才更名为最终的文件名
		tempFile, err := ioutil.TempFile("", "example")
		defer tempFile.Close()
		if err != nil {
			log.Fatalf("cannot create %v", tempFile)
		}
		inter_files_set[i] = tempFile
		inter_files_name_set[i] = tempFile.Name()
		// 为每一个中间文件创建encoder
		encoder_set[i] = json.NewEncoder(inter_files_set[i])
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 执行mapf，但此时只是对单个文件的内容做统计，还要经过hash操作到对应的中间文件中
	kva := mapf(filename, string(content))

	fmt.Printf("Mapper %v finish mapf, now write to file... \n", MapID)

	// 写入中间文件
	for _, data := range kva {
		target_reduce_id := ihash(data.Key) % NReduce
		err := encoder_set[target_reduce_id].Encode(&data)
		if err != nil {
			log.Fatalf("cannot write KV data")
		}
	}

	wd, _ := os.Getwd()
	for i := 0; i < NReduce; i++ {
		oFile := filepath.Join(wd, "mr-"+fmt.Sprint(MapID)+"-"+fmt.Sprint(i)+".json")
		// 如果存在同名文件删除
		// if _, err := os.Stat(oFile); err == nil {
		// 	// 文件存在，删除文件
		// 	fmt.Printf("Previous failed worker has created %v, now remove it.\n", "mr-"+fmt.Sprint(MapID)+"-"+fmt.Sprint(i)+".json")
		// 	err := os.Remove(oFile)
		// 	if err != nil {
		// 		log.Fatal("Failed to delete file:", err)
		// 	}
		// }
		fmt.Printf("Maper %v create file %v.\n", MapID, "mr-"+fmt.Sprint(MapID)+"-"+fmt.Sprint(i)+".json")
		err = os.Rename(inter_files_name_set[i], oFile)

	}

	fmt.Printf("Mapper %v finish dealing with %v\n", MapID, filename)

	return
}

func ReducePhase(reducef func(string, []string) string, ReduceID int) {

	kva := []KeyValue{}
	matchedFiles := []string{}

	// 获取所有ReduceID对应的中间文件mr-*-ReduceID
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println("Failed to get current directory:", err)
		return
	}

	// 后缀为ReduceID
	suffix := "-" + strconv.Itoa(ReduceID) + ".json"
	// fmt.Printf("suffix: %v\n", suffix)
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// 判断文件名是否符合要求
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), "mr-") && strings.HasSuffix(info.Name(), suffix) {
			matchedFiles = append(matchedFiles, info.Name())
		}
		return nil
	})

	if err != nil {
		fmt.Println("Failed to search for files:", err)
		return
	}

	for _, filename := range matchedFiles {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Failed to open %v\n", filename)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// 对内容排序后进行计数，可以参考mrsequential的版本
	sort.Sort(ByKey(kva))

	// 创建一个临时文件，完成后才更名为最终的文件名
	tempFile, err := ioutil.TempFile("", "example")
	defer tempFile.Close()

	tempFilePath := tempFile.Name()

	// 调用reducef并写入结果文件
	i := 0
	for i < len(kva) {
		j := i + 1
		// 把相同的key的value都聚集在一起，i和j之间的距离即表示了出现的次数，这里使用string存储"1"，所以string的长度就是统计值
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	fmt.Printf("Reducer %v finish reducef, now write to file... \n", ReduceID)

	// 只有所有的数据写入后才更名
	wd, _ := os.Getwd()
	oFile := filepath.Join(wd, "mr-out-"+strconv.Itoa(ReduceID))

	// 如果存在同名文件删除
	// if _, err := os.Stat(oFile); err == nil {
	// 	// 文件存在，删除文件
	// 	fmt.Printf("Previous failed worker has created %v, now remove it.\n", "mr-out-"+strconv.Itoa(ReduceID))
	// 	err := os.Remove(oFile)
	// 	if err != nil {
	// 		log.Fatal("Failed to delete file:", err)
	// 	}
	// }

	err = os.Rename(tempFilePath, oFile)
	if err != nil {
		log.Fatal("Failed to rename/move file:", err)
	}

	fmt.Printf("Reducer %v finish\n", ReduceID)

	return
}

func CallGetInfo() int {
	args := InfoArg{}
	reply := InfoReply{}

	state := call("Master.GetInfo", &args, &reply)

	if state == false {
		return RPC_FAILED_CODE
	}

	return reply.NReduce

}

func CallAskForTask(TaskType string) (int, string) {
	args := TaskArgs{}
	reply := TaskReply{}

	args.TaskType = TaskType

	state := call("Master.AskForTask", &args, &reply)

	if state == false {
		return RPC_FAILED_CODE, RPC_FAILED_MESSAGE
	}

	if TaskType == "map" {
		return reply.MapID, reply.File
	} else {
		return reply.ReduceID, "none"
	}

}

func CallFinishTask(TaskType string, TaskID int) int {
	args := TaskArgs{}
	reply := TaskReply{}

	args.TaskType = TaskType
	args.TaskID = TaskID

	state := call("Master.FinishTask", &args, &reply)
	if state == false {
		return RPC_FAILED_CODE
	}

	return 0
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	// 这里返回的是master所在的UNIX-domain socket
	// 因为是在同一台机器，所以就不需要走网络
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// RPC调用
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
