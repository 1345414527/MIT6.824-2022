package kvraft

import (
	"6.824/labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

//客户端
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	leaderId int
}

//用于生成一个随机数，可以生成clientId和commandId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//生成一个客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	// You'll have to add code here.
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
//根据key获取value
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	//DPrintf("%v client get key：%s.", ck.clientId, key)
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: nrand(),
	}
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			//如果请求失败，等一段时间再请求,换一个节点再请求
			DPrintf("%v client get key %v from server %v,not ok.", ck.clientId, key, leaderId)
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else if reply.Err != OK {
			DPrintf("%v client get key %v from server %v,reply err = %v!", ck.clientId, key, leaderId, reply.Err)
		}

		switch reply.Err {
		case OK:
			DPrintf("%v client get key %v from server %v,value: %v，OK.", ck.clientId, key, leaderId, reply.Value, leaderId)
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			DPrintf("%v client get key %v from server %v,NO KEY!", ck.clientId, key, leaderId)
			ck.leaderId = leaderId
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

	}
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
	DPrintf("%v client PutAppend,key：%v,value：%v,op：%v", ck.clientId, key, value, op)
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: nrand(),
	}
	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			//可能当前请求的server不是leader，换一个server再访问
			DPrintf("%v client set key %v to %v to server %v,not ok.", ck.clientId, key, value, leaderId)
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else if reply.Err != OK {
			DPrintf("%v client set key %v to %v to server %v,reply err = %v!", ck.clientId, key, value, leaderId, reply.Err)
		}

		switch reply.Err {
		case OK:
			DPrintf("%v client set key %v to %v to server %v，OK.", ck.clientId, key, value, leaderId)
			ck.leaderId = leaderId
			return
		case ErrNoKey:
			DPrintf("%v client set key %v to %v to server %v，NOKEY!", ck.clientId, key, value, leaderId)
			return
		case ErrTimeOut:
			continue
		case ErrWrongLeader:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrServer:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		default:
			log.Fatal("client rev unknown err", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
