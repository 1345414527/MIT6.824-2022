package shardkv

import (
	"6.824/shardctrler"
)

func (kv *ShardKV) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

func (kv *ShardKV) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		value = v
	} else {
		err = ErrNoKey
		value = ""
	}
	return
}

//判断能否执行客户端发来的命令
func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	//config不对
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("process key ready err config.")
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	//没有分配该shard
	if _, ok := kv.meShards[shardId]; !ok {
		kv.log("process key ready err shard.")
		return ErrWrongGroup
	}
	//正在迁移，这里有优化的空间，如果没有迁移完成，可以直接请求目标节点完成操作并返回，但是这样就太复杂了，这里简略了
	if _, ok := kv.inputShards[shardId]; ok {
		kv.log("process key ready err waitShard.")
		return ErrWrongGroup
	}
	return OK
}

//应用每一条命令
func (kv *ShardKV) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			kv.log("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh:
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				kv.log("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.lock("waitApplyCh_sn")
				kv.readPersist(false, cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
				kv.unlock("waitApplyCh_sn")
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			//处理不同的命令
			if op, ok := cmd.Command.(Op); ok {
				kv.handleOpCommand(cmdIdx, op)
			} else if config, ok := cmd.Command.(shardctrler.Config); ok {
				kv.handleConfigCommand(cmdIdx, config)
			} else if mergeData, ok := cmd.Command.(MergeShardData); ok {
				kv.handleMergeShardDataCommand(cmdIdx, mergeData)
			} else if cleanData, ok := cmd.Command.(CleanShardDataArgs); ok {
				kv.handleCleanShardDataCommand(cmdIdx, cleanData)
			} else {
				panic("apply command,NOT FOUND COMMDN！")
			}

		}

	}

}

//处理get、put、append命令
func (kv *ShardKV) handleOpCommand(cmdIdx int, op Op) {
	kv.log("start apply command %v：%+v", cmdIdx, op)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	shardId := key2shard(op.Key)
	if err := kv.ProcessKeyReady(op.ConfigNum, op.Key); err != OK {
		kv.notifyWaitCommand(op.ReqId, err, "")
		return
	}
	if op.Method == "Get" {
		//处理读
		e, v := kv.getValueByKey(op.Key)
		kv.notifyWaitCommand(op.ReqId, e, v)
	} else if op.Method == "Put" || op.Method == "Append" {
		//处理写
		//判断命令是否重复
		isRepeated := false
		if v, ok := kv.lastApplies[shardId][op.ClientId]; ok {
			if v == op.CommandId {
				isRepeated = true
			}
		}

		if !isRepeated {
			switch op.Method {
			case "Put":
				kv.data[shardId][op.Key] = op.Value
				kv.lastApplies[shardId][op.ClientId] = op.CommandId
			case "Append":
				e, v := kv.getValueByKey(op.Key)
				if e == ErrNoKey {
					//按put处理
					kv.data[shardId][op.Key] = op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				} else {
					//追加
					kv.data[shardId][op.Key] = v + op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				}
			default:
				panic("unknown method " + op.Method)
			}

		}
		//命令处理成功
		kv.notifyWaitCommand(op.ReqId, OK, "")
	} else {
		panic("unknown method " + op.Method)
	}

	kv.log("apply op: cmdId:%d, op: %+v, data:%v", cmdIdx, op, kv.data[shardId][op.Key])
	//每应用一条命令，就判断是否进行持久化
	kv.saveSnapshot(cmdIdx)
}

//处理config命令，即更新config
//主要是处理meshard、inputshard、outputshard
func (kv *ShardKV) handleConfigCommand(cmdIdx int, config shardctrler.Config) {
	kv.log("start handle config %v：%+v", cmdIdx, config)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	if config.Num <= kv.config.Num {
		kv.saveSnapshot(cmdIdx)
		return
	}

	if config.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	oldConfig := kv.config.Copy()
	outputShards := make([]int, 0, shardctrler.NShards)
	inputShards := make([]int, 0, shardctrler.NShards)
	meShards := make([]int, 0, shardctrler.NShards)

	for i := 0; i < shardctrler.NShards; i++ {
		if config.Shards[i] == kv.gid {
			meShards = append(meShards, i)
			if oldConfig.Shards[i] != kv.gid {
				inputShards = append(inputShards, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				outputShards = append(outputShards, i)
			}
		}
	}

	//处理当前的shard
	kv.meShards = make(map[int]bool)
	for _, shardId := range meShards {
		kv.meShards[shardId] = true
	}

	//处理移出的shard
	//保存当前所处配置的所有移除的shard数据
	d := make(map[int]MergeShardData)
	for _, shardId := range outputShards {
		mergeShardData := MergeShardData{
			ConfigNum:      oldConfig.Num,
			ShardNum:       shardId,
			Data:           kv.data[shardId],
			CommandIndexes: kv.lastApplies[shardId],
		}
		d[shardId] = mergeShardData
		//初始化数据
		kv.data[shardId] = make(map[string]string)
		kv.lastApplies[shardId] = make(map[int64]int64)
	}
	kv.outputShards[oldConfig.Num] = d

	//处理移入的shard
	kv.inputShards = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range inputShards {
			kv.inputShards[shardId] = true
		}
	}

	kv.config = config
	kv.oldConfig = oldConfig
	kv.log("apply op: cmdId:%d, config:%+v", cmdIdx, config)
	kv.saveSnapshot(cmdIdx)
}

//处理新的shard数据，即input shard
func (kv *ShardKV) handleMergeShardDataCommand(cmdIdx int, data MergeShardData) {
	kv.log("start merge Shard Data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	if kv.config.Num != data.ConfigNum+1 {
		return
	}

	if _, ok := kv.inputShards[data.ShardNum]; !ok {
		return
	}

	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastApplies[data.ShardNum] = make(map[int64]int64)

	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.CommandIndexes {
		kv.lastApplies[data.ShardNum][k] = v
	}
	delete(kv.inputShards, data.ShardNum)

	kv.log("apply op: cmdId:%d, mergeShardData:%+v", cmdIdx, data)
	kv.saveSnapshot(cmdIdx)
	go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}

//处理已经迁移走的shard，即output shard
func (kv *ShardKV) handleCleanShardDataCommand(cmdIdx int, data CleanShardDataArgs) {
	kv.log("start clean shard data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	//如果要清除的shard确实是在outputShard中，且没有被清除，则需要清除
	if kv.OutputDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.outputShards[data.ConfigNum], data.ShardNum)
	}

	//通知等待协程
	//if ch, ok := kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", data.ConfigNum, data.ShardNum)]; ok {
	//	ch <- struct{}{}
	//}

	kv.saveSnapshot(cmdIdx)
}
