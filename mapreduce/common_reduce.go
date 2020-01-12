package mapreduce

import (
	"encoding/json"
	"os"
	"sync"
)

func changeDataMap(myMap map[string][]string, t KeyValue){
	myMap[t.Key] = append(myMap[t.Key],t.Value)
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//Read the intermediate files from multiple mappers for the particular reducer

	outputFileObj,err := os.Create(outFile)
	if err != nil{
		return
	}
	outputEncoder := json.NewEncoder(outputFileObj)
	reductionMap := make(map[string][]string)
	i := 0
	var mapLock sync.Mutex
	var syncc sync.WaitGroup
	syncc.Add(nMap)
	//Super Parallelism
	for {
		go func(i int) {
			file,err := os.Open(reduceName(jobName,i,reduceTask))
			if err != nil{
				return
			}
			decoder := json.NewDecoder(file)
			var intermediateKVPair KeyValue
			for{
				err = decoder.Decode(&intermediateKVPair)
				if err != nil{
					break
				}
				mapLock.Lock()
				changeDataMap(reductionMap,intermediateKVPair)
				mapLock.Unlock()
			}
			file.Close()
			syncc.Done()
		}(i)
		i -= -1
		if(i == nMap){
			break
		}
	}
	syncc.Wait()

	//Run reduce function and append it to a file
	for key := range reductionMap{
		outputEncoder.Encode(KeyValue{key,reduceF(key,reductionMap[key])})
	}


	outputFileObj.Close()

}
