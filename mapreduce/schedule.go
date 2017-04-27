package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	debug("we have %d workers.\n", len(registerChan))
	i := 0
	taskChan := make(chan int, ntasks)
	for i < ntasks {
		taskChan <- i
		i++
	}
	var working sync.WaitGroup
	for {
		for len(taskChan) > 0 {
			/*
				if len(registerChan) == 0 {
					debug("Scheduler sleep 1 second.\n")
					time.Sleep(time.Second)
					continue
				}
			*/
			worker := <-registerChan
			if worker == "" {
				time.Sleep(time.Second)
				continue
			}
			taskNo := <-taskChan
			debug("debug: try to assign task %d to woker=%s\n", taskNo, worker)
			args := new(DoTaskArgs)
			args.JobName = jobName
			if phase == mapPhase {
				args.File = mapFiles[taskNo]
			}
			args.Phase = phase
			args.TaskNumber = taskNo
			args.NumOtherPhase = n_other
			working.Add(1)
			go func() {
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok == false {
					fmt.Printf("Assign pahse-%s-task-%d to worker %s failed.\n", phase, args.TaskNumber, worker)
					taskChan <- args.TaskNumber
					fmt.Printf("give task %d back to taskChannel.\n", args.TaskNumber)
				} else {
					fmt.Printf("phase-%s-task-%d is done.\n", phase, args.TaskNumber)
				}
				working.Done()
				registerChan <- worker
				debug("phase-%s-task-%d is done, worker:%s is free.\n", phase, args.TaskNumber, worker)
			}()
		}
		debug("all task are assigned.\n")
		working.Wait()
		if len(taskChan) == 0 {
			break
		}
	}
	debug("Schedule: loop is done.\n")
	close(taskChan)
	fmt.Printf("Schedule: %v phase done\n", phase)
}
