package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	State      string
	RemainTime int
}

type Master struct {
	// Your definitions here.
	NMap            int
	NReduce         int
	Files           []string
	UnfinishedCount int
	F_StartReduce   bool
	F_JobFinish     bool
	MLock           sync.Mutex
	MapID           int
	ReduceID        int
	MapTaskPool     []Task
	ReduceTaskPool  []Task
}

// Your code here -- RPC handlers for the worker to call.

const (
	UNSTARTED = "unstarted"
	RUNNING   = "running"
	FINISHED  = "finished"
	WAITTIME  = 10
)

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetInfo(args *InfoArg, reply *InfoReply) error {
	// 返回reduce的数目和当前mapper的ID，便于中间文件的命名
	reply.NReduce = m.NReduce

	return nil
}

func (m *Master) AskForTask(args *TaskArgs, reply *TaskReply) error {
	m.MLock.Lock()
	defer m.MLock.Unlock()

	GetTaskID := func(TaskPool *[]Task, length int) int {
		for i := 0; i < length; i++ {
			// 返回还没有开始的任务ID
			if (*TaskPool)[i].State == UNSTARTED {
				(*TaskPool)[i].State = RUNNING
				return i
			}
		}
		// 如果都完成了就返回-1
		return -1
	}

	if args.TaskType == "map" {
		if m.F_StartReduce == true {
			reply.MapID = -2
			reply.File = "NoFile"
		} else {
			reply.MapID = GetTaskID(&m.MapTaskPool, m.NMap)
			if reply.MapID != -1 {
				reply.File = m.Files[reply.MapID]
			}
		}
	} else {
		// 如果还没开始Reduce，返回-2，让worker等待
		// 如果已经开始Reduce，若reduce任务数已满，返回-1告诉worker可以不用工作了，否则返回对应的ReduceID
		if m.F_StartReduce == false {
			reply.ReduceID = -2
		} else {
			reply.ReduceID = GetTaskID(&m.ReduceTaskPool, m.NReduce)
		}
	}

	return nil
}

func (m *Master) FinishTask(args *TaskArgs, reply *TaskReply) error {
	m.MLock.Lock()
	defer m.MLock.Unlock()

	if args.TaskType == "map" {
		m.MapTaskPool[args.TaskID].State = FINISHED
		m.UnfinishedCount -= 1

		fmt.Printf("Map task %v finished, unfinished count: %v\n", args.TaskID, m.UnfinishedCount)
		if m.UnfinishedCount == 0 {
			fmt.Printf("All map task finished. Start reducing...\n")
			m.F_StartReduce = true
			m.UnfinishedCount = m.NReduce
		}
	} else {
		m.ReduceTaskPool[args.TaskID].State = FINISHED
		m.UnfinishedCount -= 1
		fmt.Printf("Reduce task finished, unfinished count: %v\n", m.UnfinishedCount)
		if m.UnfinishedCount == 0 {
			fmt.Printf("All reduce task finished. Job finished.\n")
			m.F_JobFinish = true
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 周期性判断是否有超时的函数
func OutTimeJudge(m *Master) {
	Judge := func(TaskPool *[]Task, length int) {
		var outtimeIDs []int
		for i := 0; i < length; i++ {
			// 返回还没有开始的任务ID
			if (*TaskPool)[i].State == RUNNING {
				(*TaskPool)[i].RemainTime -= 1
				if (*TaskPool)[i].RemainTime == 0 {
					(*TaskPool)[i] = Task{State: UNSTARTED, RemainTime: WAITTIME}
					outtimeIDs = append(outtimeIDs, i)
				}
			}
		}
		fmt.Printf("outtime IDs:")
		for j := 0; j < len(outtimeIDs); j++ {
			fmt.Printf("%v ", outtimeIDs[j])
		}
		fmt.Printf("\n")
	}

	for {
		if m.F_StartReduce == false {
			Judge(&m.MapTaskPool, m.NMap)
		} else {
			Judge(&m.ReduceTaskPool, m.NReduce)
		}
		time.Sleep(time.Second)
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.F_JobFinish == true {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.

	len_files := len(files)

	MapTaskPool := make([]Task, len_files)
	ReduceTaskPool := make([]Task, nReduce)

	for i := 0; i < len_files; i++ {
		MapTaskPool[i] = Task{State: UNSTARTED, RemainTime: WAITTIME}
	}

	for i := 0; i < nReduce; i++ {
		ReduceTaskPool[i] = Task{State: UNSTARTED, RemainTime: WAITTIME}
	}

	m := Master{
		NMap:            len_files,
		NReduce:         nReduce,
		Files:           files,
		UnfinishedCount: len_files,
		F_StartReduce:   false,
		F_JobFinish:     false,
		MLock:           sync.Mutex{},
		MapID:           0,
		ReduceID:        0,
		MapTaskPool:     MapTaskPool,
		ReduceTaskPool:  ReduceTaskPool,
	}

	// 周期性检查任务是否完成，如果超时重新分配
	go OutTimeJudge(&m)

	m.server()

	return &m
}
