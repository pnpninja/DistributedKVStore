package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
	"unsafe"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation  string
	Key        string
	Value      string
	Info 	   RaftLogInfo
}

type Reply struct {
	SequenceNumber 	int64
	Reply 	GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	kill bool
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	serverHashMap            map[string]string
	recentReplies            map[int64]*Reply
	notificationInterfaceMap map[int64]chan interface{}

}

//Source - https://stackoverflow.com/questions/35790935/using-reflection-in-go-to-get-the-name-of-a-struct
//Using this to prevent too many repetitive codeblocks
func getType(myvar interface{}) string {
	valueOf := reflect.ValueOf(myvar)
	if valueOf.Type().Kind() == reflect.Ptr {
		return reflect.Indirect(valueOf).Type().Name()
	} else {
		return valueOf.Type().Name()
	}
}

func (kv *KVServer) checkIfItIsNotLeaderAndPrepareResponse(reply interface{})(bool){

	_, isItLeader := kv.rf.GetState()
	if isItLeader == false {
		if(getType(reply) == "GetReply"){
			temp := *((*GetReply)(unsafe.Pointer(&reply)))
			temp.Err = ""
			temp.WrongLeader = false
		} else{
			temp := *((*PutAppendReply)(unsafe.Pointer(&reply)))
			temp.Err = ""
			temp.WrongLeader = false
		}
		return true
	}else{
		return false
	}
}

func (kv *KVServer) checkBasedOnClientIdentificationAndNumberSequenceNumber(cid int64, sid int64, reply interface{}) (bool,map[string]interface{}){
	kv.mu.Lock()
	mapp := make(map[string]interface{})
	_, exists := kv.recentReplies[cid]
	if(exists){
		latReply := kv.recentReplies[cid]
		if(sid > latReply.SequenceNumber){
			kv.mu.Unlock()
			return false,mapp
		}else{
			mapp["WrongLeader"] = true
			mapp["Err"] = latReply.Reply.Err
			if(getType(reply) == "GetReply") {
				mapp["Value"] = latReply.Reply.Value
			}
			kv.mu.Unlock()
			return true,mapp
		}
	}else{
		kv.mu.Unlock()
		return false,mapp
	}
}


func (kv *KVServer) createOperationObject(operation string, key string, value string, clientid int64, seqid int64) Op{
	newobj := Op{
		Operation: operation,
		Key: key,
		Value: value,
		Info:RaftLogInfo{
			ClientIdentificationNumber: clientid,
			SequenceNumber:             seqid,
		},
	}
	return newobj
}

func (kv *KVServer)createNewChannelForIndex(ind int, newchannel *(chan interface{}))  {
	kv.mu.Lock()
	kv.notificationInterfaceMap[int64(ind)] = *newchannel
	kv.mu.Unlock()

}

func (kv *KVServer) processEntryPutAppend(oldTerm int, currentIndex int, reply *PutAppendReply){
	curTerm, isLeader := kv.rf.GetState()
	if !(isLeader && curTerm == oldTerm) {
		reply.WrongLeader = false
		reply.Err = ""
	} else {
		reply.WrongLeader = true
		reply.Err = "OK"
	}
}

func (kv *KVServer) processEntryGet(oldTerm int, index int, args *GetArgs,reply *GetReply){
	curTerm, isLeader := kv.rf.GetState()
	if !(isLeader && curTerm == oldTerm) {
		reply.WrongLeader = false
		reply.Err = ""
	} else {
		kv.mu.Lock()
		reply.WrongLeader = true
		_, isPresent := kv.serverHashMap[args.Key]
		if(isPresent){
			data := kv.serverHashMap[args.Key]
			reply.Value = data
			reply.Err = "OK"
		}else{
			reply.Err = "ErrNoKey"
		}
		kv.mu.Unlock()
	}
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	isItNotLeader := kv.checkIfItIsNotLeaderAndPrepareResponse(reply)
	if isItNotLeader{
		return
	}

	checkFailed,data := kv.checkBasedOnClientIdentificationAndNumberSequenceNumber(args.Info.ClientIdentificationNumber,args.Info.SequenceNumber,reply)
	if(checkFailed){
		reply.WrongLeader = data["WrongLeader"].(bool)
		reply.Value = data["Value"].(string)
		reply.Err = data["Err"].(Err)
		return
	}

	cmd := kv.createOperationObject("Get",args.Key,"",args.Info.ClientIdentificationNumber,args.Info.SequenceNumber)
	newchannel := make(chan interface{})
	currentIndex, oldTerm, _ := kv.rf.Start(cmd)
	kv.createNewChannelForIndex(currentIndex,&newchannel)

	select {
	case <-newchannel:
		kv.processEntryGet(oldTerm, currentIndex, args ,reply)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	isItNotLeader := kv.checkIfItIsNotLeaderAndPrepareResponse(reply)
	if isItNotLeader{
		return
	}

	checkFailed,data1 := kv.checkBasedOnClientIdentificationAndNumberSequenceNumber(args.Info.ClientIdentificationNumber,args.Info.SequenceNumber,reply)
	if(checkFailed){
		reply.WrongLeader = data1["WrongLeader"].(bool)
		reply.Err = data1["Err"].(Err)
		return
	}

	cmd := kv.createOperationObject(args.Op,args.Key,args.Value,args.Info.ClientIdentificationNumber,args.Info.SequenceNumber)
	currentIndex, oldTerm, _ := kv.rf.Start(cmd)
	newchannel := make(chan interface{})
	kv.createNewChannelForIndex(currentIndex,&newchannel)

	select {
	case <-newchannel:
		kv.processEntryPutAppend(oldTerm,currentIndex,reply)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.kill = true
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *KVServer) initializeDataStructures(){
	kv.serverHashMap = make(map[string]string)
	kv.recentReplies = make(map[int64]*Reply)
	kv.notificationInterfaceMap = make(map[int64]chan interface{})
}
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.initializeDataStructures()
	go kv.handleIncomingApplyChannelData()
	return kv
}

//Sauce - https://medium.com/@matryer/golang-advent-calendar-day-two-starting-and-stopping-things-with-a-signal-channel-f5048161018
//https://golang.org/ref/spec#Type_assertions


func (kv *KVServer) handleIncomingApplyChannelData()  {

	for appliedEntry := range kv.applyCh {
		kv.mu.Lock()
		command := appliedEntry.Command.(Op) //Typecasting
		toProcess := false
		_,hasData := kv.recentReplies[command.Info.ClientIdentificationNumber]
		if(hasData){
			brandNewData := kv.recentReplies[command.Info.ClientIdentificationNumber]
			latestSeqNumber := brandNewData.SequenceNumber
			if(!(command.Info.SequenceNumber <= latestSeqNumber)){
				toProcess = true
			}else{
				toProcess = false
			}
		}else{
			toProcess = true
		}
		if toProcess == true {
			latestReply := Reply{SequenceNumber: command.Info.SequenceNumber}
			kv.recentReplies[command.Info.ClientIdentificationNumber] = &latestReply
			if(command.Operation == "Get"){
				reply := GetReply{}
				_, isPresent :=  kv.serverHashMap[command.Key]
				if isPresent {
					data := kv.serverHashMap[command.Key]
					reply.Value = data
				}else{
					reply.Err = "ErrNoKey"
				}
				latestReply.Reply = reply
			}else if(command.Operation == "Put"){
				kv.serverHashMap[command.Key] = command.Value

			}else{
				kv.serverHashMap[command.Key] += command.Value
			}

		}

		_,ok3 := kv.notificationInterfaceMap[int64(appliedEntry.CommandIndex)]
		if(ok3){
			ch := kv.notificationInterfaceMap[int64(appliedEntry.CommandIndex)]
			if(ch != nil){
				close(ch)
				delete(kv.notificationInterfaceMap, int64(appliedEntry.CommandIndex))
			}
		}
		kv.mu.Unlock()
	}





}