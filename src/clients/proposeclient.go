package clients

import (
	"clientproto"
	"fastrpc"
	"genericsmrproto"
	"state"
)

type ProposeClient struct {
	*AbstractClient
	proposeReplyChan chan fastrpc.Serializable
	propose          *genericsmrproto.Propose
	opCount          int32
	fast             bool
	noLeader         bool
}

func NewProposeClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string,
	fast bool, noLeader bool, replyChan chan fastrpc.Serializable) *ProposeClient {
	pc := &ProposeClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		replyChan,                                                    // proposeReplyChan
		new(genericsmrproto.Propose),                                 // propose
		0,                                                            // opCount
		fast,                                                         // fast
		noLeader,                                                     // noLeader
	}
	pc.RegisterRPC(new(genericsmrproto.ProposeReplyTS), clientproto.GEN_PROPOSE_REPLY,
		pc.proposeReplyChan)
	if noLeader {
		pc.ConnectToReplicas()
		pc.DetermineReplicaPings()
	}
	return pc
}

func (c *ProposeClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64){
	// reqStart := time.Now()
	for i, opType := range opTypes {
		k := keys[i]

		// before := time.Now()
		// var opTypeStr string
		// var success bool
		if opType == state.GET {
			// opTypeStr = "read"
			c.Read(k)
		} else if opType == state.PUT {
			// opTypeStr = "write"
			c.Write(k, int64(k))
		} else {
			// opTypeStr = "rmw"
			c.CompareAndSwap(k, int64(k-1), int64(k))
		}
		// after := time.Now()s

		// if !success {
		// 	return false, -1
		// 	// lat := after.Sub(before).Nanoseconds()
		// 	// fmt.Printf("%s,%d,%d,%d\n", opTypeStr, lat, k, i)
		// }
	}
	
	// reqEnd := time.Now()
	// log.Printf("App request took %.2f seconds.", reqEnd.Sub(reqStart).Seconds())
	return true, 0
}

func (c *ProposeClient) Read(key int64) (bool, int64) {
	commandId := c.id * 1000 + c.opCount
	c.opCount++
	c.preparePropose(commandId, key, 0)
	c.propose.Command.Op = state.GET
	c.sendPropose()
	return true, 0
}

func (c *ProposeClient) Write(key int64, value int64) bool {
	commandId := c.id * 1000 + c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = state.PUT
	c.sendPropose()
	return true
}

func (c *ProposeClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	commandId := c.id * 1000 + c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.propose.Command.Op = state.CAS
	c.sendPropose()
	return true, 0
}

func (c *ProposeClient) preparePropose(commandId int32, key int64, value int64) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
}

func (c *ProposeClient) sendProposeAndReadReply() (bool, int64) {
	c.sendPropose()
	return c.readProposeReply(c.propose.CommandId)
}

func (c *ProposeClient) sendPropose() {
	if !c.fast {
		shard := c.GetShardFromKey(c.propose.Command.K)
		replica := shard
		if c.noLeader {
			if c.forceLeader >= 0 {
				replica = c.forceLeader
			} else {
				replica = int(c.replicasByPingRank[shard][0])
			}
			c.replicaWriters[shard][replica].WriteByte(clientproto.GEN_PROPOSE)
			c.propose.Marshal(c.replicaWriters[shard][replica])
			c.replicaWriters[shard][replica].Flush()
		} else {
			c.writers[replica].WriteByte(clientproto.GEN_PROPOSE)
			c.propose.Marshal(c.writers[replica])
			c.writers[replica].Flush()
		}
		
	} else {
		for i := 0; i < c.numLeaders; i++ {
			c.writers[i].WriteByte(clientproto.GEN_PROPOSE)
			c.propose.Marshal(c.writers[i])
			c.writers[i].Flush()
		}
	}
}

func (c *ProposeClient) readProposeReply(commandId int32) (bool, int64) {
	for !c.shutdown {
		reply := (<-c.proposeReplyChan).(*genericsmrproto.ProposeReplyTS)
		if reply.OK == 0 {
			return false, 0
		} else {
			//dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			if commandId == reply.CommandId {
				return true, int64(reply.Value)
			}
		}
	}
	return false, 0
}
