package shardkv

import "6.824/labgob"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

//回复状态码
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrServer      = "ErrServer"
)

type Err string

//主要是applyCh的处理中，ApplyMsg的Command是一个interface，因此要向labgob注册具体实现才能进行编解码
func init() {
	//labgob.Register(PutAppendArgs{})
	//labgob.Register(PutAppendReply{})
	//labgob.Register(GetArgs{})
	//labgob.Register(GetReply{})
	//labgob.Register(FetchShardDataArgs{})
	//labgob.Register(FetchShardDataReply{})
	labgob.Register(CleanShardDataArgs{})
	//labgob.Register(CleanShardDataReply{})
	labgob.Register(MergeShardData{})
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

func (c *PutAppendArgs) copy() PutAppendArgs {
	r := PutAppendArgs{
		Key:       c.Key,
		Value:     c.Value,
		Op:        c.Op,
		ClientId:  c.ClientId,
		CommandId: c.CommandId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

func (c *GetArgs) copy() GetArgs {
	r := GetArgs{
		Key:       c.Key,
		ClientId:  c.ClientId,
		CommandId: c.CommandId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

//用于向目标节点获取input shard
type FetchShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type FetchShardDataReply struct {
	Success        bool
	CommandIndexes map[int64]int64
	Data           map[string]string
}

func (reply *FetchShardDataReply) Copy() FetchShardDataReply {
	res := FetchShardDataReply{
		Success:        reply.Success,
		Data:           make(map[string]string),
		CommandIndexes: make(map[int64]int64),
	}
	for k, v := range reply.Data {
		res.Data[k] = v
	}
	for k, v := range reply.CommandIndexes {
		res.CommandIndexes[k] = v
	}
	return res
}

//用于请求目标节点清除指定的output shard
type CleanShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type CleanShardDataReply struct {
	Success bool
}

//用于存储output shard的数据，以及充当input shard在apply的命令
type MergeShardData struct {
	ConfigNum      int
	ShardNum       int
	CommandIndexes map[int64]int64 //当前shard的所有客户端的最后一条命令id
	Data           map[string]string
}
