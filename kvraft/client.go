//Citations - https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf

package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Info RaftLogInfo
}


func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Info.SequenceNumber = 1
	ck.Info.ClientIdentificationNumber = nrand()
	return ck

}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	get := &GetArgs{
		Key:  key,
		//There is no deep copy of struct by default. Need to look into it. Sucks doing this
		Info: RaftLogInfo{
			ClientIdentificationNumber: ck.Info.ClientIdentificationNumber,
			SequenceNumber:             ck.Info.SequenceNumber,
		},
	}


	for serverNumber := 0; serverNumber > -1; serverNumber = (serverNumber + 1)%len(ck.servers){

		//https://gobyexample.com/timeouts
		responseChannel := make(chan bool, 1)
		response := new(GetReply)
		go func(serverNumberr int) {
			ok := ck.servers[serverNumberr].Call("KVServer.Get", get, response)
			responseChannel <- ok
		}(serverNumber)
		select {
		case <-time.After(450 * time.Millisecond):
			//Didnt receive anything . Nothing can be done. Try again
			continue
		case ok := <-responseChannel:
			if ok && response.WrongLeader != false {
				ck.Info.SequenceNumber -= -1
				return response.Value
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppend := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		//There is no deep copy of struct by default
		Info:  RaftLogInfo{
			ClientIdentificationNumber: ck.Info.ClientIdentificationNumber,
			SequenceNumber:             ck.Info.SequenceNumber,
		},
	}

	for serverNumber := 0; serverNumber > -1; serverNumber = (serverNumber + 1)%len(ck.servers){

		//https://gobyexample.com/timeouts
		gryzzl := make(chan bool, 1)
		response := new(PutAppendReply)
		go func(servernumberrr int) {
			ok := ck.servers[servernumberrr].Call("KVServer.PutAppend", putAppend, response)
			gryzzl <- ok
		}(serverNumber)

		select {
		case <-time.After(450 * time.Millisecond):
			//Didnt receive anything . Nothing can be done. Try again
			continue
		case ok := <-gryzzl:
			if ok && response.WrongLeader != false {
				ck.Info.SequenceNumber -= -1
				return
			}else{

			}
		}

	}
}



func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
