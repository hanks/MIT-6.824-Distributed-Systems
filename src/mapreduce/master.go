package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	_ = "breakpoint"
	var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)

	var SendMap = func(worker string, jobNum int) bool {
		args := &DoJobArgs{
			NumOtherPhase: mr.nReduce,
			File:          mr.file,
			Operation:     Map,
			JobNumber:     jobNum,
		}

		var reply DoJobReply
		return call(worker, "Worker.DoJob", args, &reply)
	}

	var SendReduce = func(worker string, jobNum int) bool {
		args := &DoJobArgs{
			NumOtherPhase: mr.nMap,
			File:          mr.file,
			Operation:     Reduce,
			JobNumber:     jobNum,
		}

		var reply DoJobReply
		return call(worker, "Worker.DoJob", args, &reply)
	}

	for i := 0; i < mr.nMap; i++ {
		go func(jobNum int) {
			var worker string
			var ok bool = false

			select {
			case worker = <-mr.idleChannel:
				ok = SendMap(worker, jobNum)
			case worker = <-mr.registerChannel:
				ok = SendMap(worker, jobNum)
			}

			if ok == true {
				mapChan <- jobNum
				mr.idleChannel <- worker
				return
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(jobNum int) {
			var worker string
			var ok bool = false

			select {
			case worker = <-mr.idleChannel:
				ok = SendReduce(worker, jobNum)
			case worker = <-mr.registerChannel:
				ok = SendReduce(worker, jobNum)
			}

			if ok == true {
				reduceChan <- jobNum
				mr.idleChannel <- worker
				return
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}

	return mr.KillWorkers()
}
