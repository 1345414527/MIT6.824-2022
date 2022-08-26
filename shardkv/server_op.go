package shardkv

import (
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 //用来标识commandNotify
	CommandId int64
	ClientId  int64
	Key       string
	Value     string
	Method    string
	ConfigNum int
}

type CommandResult struct {
	Err   Err
	Value string
}

func (kv *ShardKV) removeCh(reqId int64) {
	kv.lock("removeCh")
	if _, ok := kv.commandNotifyCh[reqId]; ok {
		delete(kv.commandNotifyCh, reqId)
	}
	kv.unlock("removeCh")
}

/*
Get和PutAppend RPC的处理
*/

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	res := kv.waitCommand(args.ClientId, args.CommandId, "Get", args.Key, "", args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	res := kv.waitCommand(args.ClientId, args.CommandId, args.Op, args.Key, args.Value, args.ConfigNum)
	reply.Err = res.Err
}

func (kv *ShardKV) waitCommand(clientId int64, commandId int64, method, key, value string, configNum int) (res CommandResult) {
	kv.log("wait cmd start,clientId：%d,commandId: %d,method: %s,key-value:%s %s,configNum %d", clientId, commandId, method, key, value, configNum)
	op := Op{
		ReqId:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Method:    method,
		Key:       key,
		ConfigNum: configNum,
		Value:     value,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		kv.log("wait cmd NOT LEADER.")
		return
	}
	kv.lock("waitCommand")
	ch := make(chan CommandResult, 1)
	kv.commandNotifyCh[op.ReqId] = ch
	kv.unlock("waitCommand")
	kv.log("wait cmd notify,index: %v,term: %v,op: %+v", index, term, op)
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case <-t.C:
		res.Err = ErrTimeOut
	case res = <-ch:
	case <-kv.stopCh:
		res.Err = ErrServer
	}

	kv.removeCh(op.ReqId)
	kv.log("wait cmd end,Op: %+v.res：%+v", op, res)
	return

}
