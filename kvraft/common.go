package raftkv

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	Info RaftLogInfo
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Info RaftLogInfo
}

type RaftLogInfo struct{
	ClientIdentificationNumber int64
	SequenceNumber             int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
