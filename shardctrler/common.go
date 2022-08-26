package shardctrler

import (
	"6.824/labgob"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

type Err string

// The number of shards.
const NShards = 10

//状态码
const (
	OK             = "OK"
	ErrWrongLeader = "wrongLeader"
	ErrTimeout     = "timeout"
	ErrServer      = "ErrServer"
)

//必须注册才能进行解码和编码
func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
//保存配置信息
type Config struct {
	Num    int              // config number，当前配置的编号
	Shards [NShards]int     // shard -> gid，每一个分片到replica group id的映射
	Groups map[int][]string // gid -> servers[]，每一个replica group包含哪些server
}

type ClientCommandId struct {
	ClientId  int64
	CommandId int64
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientCommandId
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientCommandId
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientCommandId
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientCommandId
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (c *Config) Copy() Config {
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}
