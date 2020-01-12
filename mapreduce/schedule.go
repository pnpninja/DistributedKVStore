package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	//Lets create a sync.WaitGroup that has as many jobs as ntasks
	var forkJoinLock sync.WaitGroup
	forkJoinLock.Add(ntasks)

	//Now, iterate through each task
	for i := 0; i < ntasks; i++{
		var workerArgs DoTaskArgs
		workerArgs.JobName = jobName
		workerArgs.Phase = phase
		workerArgs.NumOtherPhase = n_other
		workerArgs.TaskNumber = i
		//Only mapPhase has files
		//Research into why go doesn't support one line If-else statements
		if(phase == mapPhase){
			workerArgs.File = mapFiles[i]
		}

		go func() {
			isThreadSuccessful := false
			//Ensure that task is definitely completed. Otherwise , major repurcussions in handling workers
			Loop:
			for{
				//Get a fresh worker if last thread was unsuccessful. This seems to be blocked if there are no workers in channel
				worker := <-registerChan
				isThreadSuccessful = call(worker,"Worker.DoTask",workerArgs,nil)
				switch isThreadSuccessful {
				case true:
					go func(){
						registerChan <- worker
					}()
					forkJoinLock.Done()
					break Loop
				case false:
					go func(){
						registerChan <- worker
					}()
				}
			}
		}()
	}
	//For synchronization of all tasks
	forkJoinLock.Wait()
}
