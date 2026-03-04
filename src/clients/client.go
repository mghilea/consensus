package clients

import (
	"bufio"
	"clientproto"
	"coordinatorproto"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"io"
	"log"
	"masterproto"
	"net"
	"net/rpc"
	"state"
	"stats"
	"time"
	"dlog"
)

const CHAN_BUFFER_SIZE = 200000

const TIMEOUT_SECS = 10
const MAX_RETRIES = 5

type ReplicaReply struct {
	reply   interface{}
	replica int
	err     error
}

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Client interface {
	Read(key int64) (bool, int64)
	Write(key int64, value int64) bool
	CompareAndSwap(key int64, oldValue int64,
		newValue int64) (bool, int64)
	AppRequest(opTypes []state.Operation, keys []int64) (bool, int64)
	Finish()
	ConnectToCoordinator()
	// ConnectToReplicas()
	// DetermineLeader()
	// DetermineReplicaPings()
	// DelayRPC(replica int, opCode uint8)
}

type AbstractClient struct {
	ctx                MachineContext
	id                 int32
	coordinatorAddr    string
	coordinatorPort    int
	forceLeader        int
	stats              *stats.StatsMap
	statsFile          string
	rpcTable           map[uint8]*RPCPair
	glArgs             *masterproto.GetLeaderArgs
	glReply            *masterproto.GetLeaderReply
	replyChan          chan *ReplicaReply
	leaderAddrs        []string
	numLeaders         int
	leaders            []*SharedConn
	shutdown           bool
	leader             int
	pingReplyChan      chan fastrpc.Serializable
	leaderAlive        []bool
	replicaPing        [][]uint64
	replicasByPingRank [][]int32
	retries            []int
	delayRPC           []map[uint8]bool
	delayedRPC         []map[uint8]chan fastrpc.Serializable
	replicasPerShard   [][]string
	replicaConns       [][]*SharedConn
	replicaRetries     [][]int
	replicaAlive       [][]bool
}

func NewAbstractClient(ctx MachineContext, id int32, coordinatorAddr string, coordinatorPort int, forceLeader int, statsFile string) *AbstractClient {
	c := &AbstractClient{
		ctx,                           // machineContext
		id,                            // id
		coordinatorAddr,               // coordinatorAddr
		coordinatorPort,               // coordinatorPort
		forceLeader,                   // forceLeader
		stats.NewStatsMap(),           // stats
		statsFile,                     // statsFile
		make(map[uint8]*RPCPair),      // rpcTable
		&masterproto.GetLeaderArgs{},  // glArgs
		&masterproto.GetLeaderReply{}, // glReply
		make(chan *ReplicaReply),      // replyChan
		make([]string, 0),             // replicaAddrs
		-1,                            // numReplicas
		make([]*SharedConn, 0),        // replicas
		false,                         // shutdown
		-1,                            // leader
		make(chan fastrpc.Serializable, // pingReplyChan
			CHAN_BUFFER_SIZE),
		make([]bool, 0),           // replicasAlive
		make([][]uint64, 0),       // replicaPing
		make([][]int32, 0),        // replicasByPingRank
		make([]int, 0),            // retries
		make([]map[uint8]bool, 0), // delayRPC
		make([]map[uint8]chan fastrpc.Serializable, 0), // delayedRPC
		make([][]string, 0),             // replicasPerShard
		make([][]*SharedConn, 0),     // replicaConns
		make([][]int, 0),                // replicaRetries
		make([][]bool, 0),               // replicasAlive
	}
	c.RegisterRPC(new(clientproto.PingReply), clientproto.GEN_PING_REPLY, c.pingReplyChan)

	c.leaderAddrs = c.ctx.Config.LeaderAddrs
	c.numLeaders = len(c.leaderAddrs)
	c.replicasPerShard = c.ctx.Config.ReplicaAddrsPerShard
	c.transport = c.ctx.Transport	
	c.leaderAlive = make([]bool, c.numLeaders)
	c.replicaPing = make([][]uint64, c.numLeaders)
	c.replicasByPingRank = make([][]int32, c.numLeaders)
	c.retries = make([]int, c.numLeaders)
	c.ConnectToShards()
	// c.DetermineLeader()
	// c.DetermineReplicaPings()

	return c
}

func (c *AbstractClient) Finish() {
	if !c.shutdown {
		c.shutdown = true
		if len(c.statsFile) > 0 {
			c.stats.Export(c.statsFile)
		}
		for _, replica := range c.leaders {
			replica.Close()
		}
	}
}

func (c *AbstractClient) ConnectToShards() {
	log.Printf("Connecting to shards using shared transport...\n")

	c.leaders = make([]*SharedConn, c.numLeaders)

	transport := c.ctx.Transport
	if transport == nil {
		log.Fatalf("Transport is nil in MachineContext")
	}

	for i := 0; i < c.numLeaders; i++ {
		if c.leaders[i].Conn != nil {
			c.leaders[i].Conn.Close()
		}
		c.retries[i]++
		log.Printf("Dialing leader %d with addr %s\n", i, c.leaderAddrs[i])

		addr := c.ctx.Config.LeaderAddrs[i]

		sharedConn, err := transport.Get(addr)
		if err != nil {
			log.Fatalf("Error connecting to leader %s: %v\n", addr, err)
		}

		// Store shared connection
		c.leaders[i] = sharedConn
		log.Printf("Connected to leader %d with connection %s\n", i, c.leaders[i].Conn.LocalAddr().String())

		// Send client ID handshake
		var idBytes [4]byte
		binary.LittleEndian.PutUint32(idBytes[:], uint32(c.id))

		if _, err := c.leaders[i].Writer.Write(idBytes[:]); err != nil {
			log.Fatalf("Failed writing client ID to leader %d: %v", i, err)
		}

		if err := c.leaders[i].Writer.Flush(); err != nil {
			log.Fatalf("Failed flushing client ID to leader %d: %v", i, err)
		}

		// Start listener
		c.leaderAlive[i] = true
		go c.leaderListener(i)
	}

	log.Printf("Successfully connected to all %d leaders.\n", c.numLeaders)
}

func (c *AbstractClient) ConnectToReplicas() {

	log.Printf("Connecting to replicas using shared transport...\n")

	transport := c.ctx.Transport
	if transport == nil {
		log.Fatalf("Transport is nil in MachineContext")
	}

	c.replicaConns = make([][]*SharedConn, c.numLeaders)
	c.replicaRetries = make([][]int, c.numLeaders)
	c.replicaAlive = make([][]bool, c.numLeaders)

	for i := 0; i < c.numLeaders; i++ {
		shardReplicas := c.ctx.Config.ReplicaAddrsPerShard[i]
		c.replicaConns[i] = make([]*SharedConn, len(shardReplicas))
		c.replicaRetries[i] = make([]int, len(shardReplicas))
		c.replicaAlive[i] = make([]bool, len(shardReplicas))

		for j := 0; j < len(shardReplicas); j++ {

			addr := shardReplicas[j]

			if c.replicaConns[i][j] != nil {
				c.replicaConns[i][j].Close()
			}
			c.replicaRetries[i][j]++
			log.Printf("Dialing shard %d replica %d with addr %s\n", i, j, addr)

			sharedConn, err := transport.Get(addr)
			if err != nil {
				log.Fatalf("Error connecting to replica %s: %v\n", addr, err)
			}
			log.Printf("Connected to shard %d replica %d with connection %s\n", i, j, addr)

			c.replicaConns[i][j] = sharedConn.Conn
			c.replicaAlive[i][j] = true

			// Send client ID handshake once
			var idBytes [4]byte
			binary.LittleEndian.PutUint32(idBytes[:], uint32(c.id))

			if _, err := c.replicaConns[i][j].Writer.Write(idBytes[:]); err != nil {
				log.Fatalf("Failed writing client ID to replica %s: %v", addr, err)
			}
			if err := c.replicaConns[i][j].Writer.Flush(); err != nil {
				log.Fatalf("Failed flushing client ID to replica %s: %v", addr, err)
			}

			// Start listener
			go c.replicaListener(i, j)
		}
	}

	log.Printf("Successfully connected to all replicas.\n")
}

func (c *AbstractClient) GetShardFromKey(k state.Key) int {
	nShards := len(c.leaders)
	return int(k) % nShards
}

func (c *AbstractClient) DetermineReplicaPings() {
	c.replicaPing = make([][]uint64, c.numLeaders)
	for i := range(c.replicaPing){
		c.replicaPing[i] = make([]uint64, len(c.replicasPerShard[i]))
	}
	c.replicasByPingRank = make([][]int32, c.numLeaders)
	for i := range(c.replicasByPingRank){
		c.replicasByPingRank[i] = make([]int32, len(c.replicasPerShard[i]))
	}

	log.Printf("Determining replica pings...\n")

	done := make(chan bool, len(c.replicasPerShard)*len(c.replicasPerShard[0]))

	for i := 0; i < len(c.replicasPerShard); i++ {
		for j := 0; j < len(c.replicasPerShard[0]); j++{
			go c.pingReplica(i, j, done)
		}
	}

	for i := 0; i < len(c.replicasPerShard)*len(c.replicasPerShard[0]); i++ {
		if ok := <-done; !ok {
			log.Fatalf("Must successfully ping all replicas on startup.\n")
		}
	}

	// mini selection sort
	for i := range c.replicasByPingRank {
		for j := range c.replicasByPingRank[i] {
			c.replicasByPingRank[i][j] = int32(j)
		}
	}

	for i := 0; i < len(c.replicasByPingRank); i++ {
		rank := c.replicasByPingRank[i]
		log.Printf("ReplicaPing[%d] = %v\n", i, c.replicaPing[i])
		for j := 0; j < len(rank); j++ {
			minIndex := j
			for k := j + 1; k < len(rank); k++ {
				if c.replicaPing[i][rank[k]] < c.replicaPing[i][rank[minIndex]] {
					minIndex = k
				}
			}
			rank[j], rank[minIndex] = rank[minIndex], rank[j]
		}
		c.replicasByPingRank[i] = rank
		log.Printf("Ordered replicasByPingRank[%d] = %v\n", i, rank)
	}
	log.Printf("Successfully pinged all replicas!\n")
}

func (c *AbstractClient) pingReplica(i int, j int, done chan bool) {
	var err error
	log.Printf("Sending ping to replica %s\n", c.replicasPerShard[i][j])
	err = c.replicaConns[i][j].Writer.WriteByte(clientproto.GEN_PING)
	log.Printf("Wrote GEN_PING opcode to replica %s\n", c.replicasPerShard[i][j])
	if err != nil {
		log.Printf("Error writing GEN_PING opcode to replica %d: %v\n", c.replicasPerShard[i][j], err)
		done <- false
		return
	}
	ping := &clientproto.Ping{c.id, uint64(time.Now().UnixNano())}
	ping.Marshal(c.replicaConns[i][j].Writer)
	err = c.replicaConns[i][j].Writer.Flush()
	log.Printf("Flushed ping to replica %d\n", c.replicasPerShard[i][j])
	if err != nil {
		log.Printf("Error flushing connection to replica %d: %v\n", c.replicasPerShard[i][j], err)
		done <- false
		return
	}
	select {
	case pingReplyS := <-c.pingReplyChan:
		pingReply := pingReplyS.(*clientproto.PingReply)
		c.replicaPing[pingReply.ShardId][pingReply.ReplicaId] = uint64(time.Now().UnixNano()) -
			pingReply.Ts
		log.Printf("Received ping from shard %d replica %d in time %d\n",
			pingReply.ShardId, pingReply.ReplicaId, c.replicaPing[pingReply.ShardId][pingReply.ReplicaId])
		done <- true
		log.Printf("Done pinging shard %d replica %d.\n", i, j)
		break
	case <-time.After(TIMEOUT_SECS * time.Second):
		log.Printf("Timeout out pinging shard %d replica %d.\n", i, j)
		for c.retries[i] < MAX_RETRIES {
			if c.connectToReplica(i, j) {
				c.pingReplica(i, j, done)
				return
			}
		}
		done <- false
		break
	}
}

func (c *AbstractClient) RegisterRPC(msgObj fastrpc.Serializable,
	opCode uint8, notify chan fastrpc.Serializable) {
	c.rpcTable[opCode] = &RPCPair{msgObj, notify}
}

func (c *AbstractClient) DelayRPC(replica int, opCode uint8) {
	_, ok := c.delayedRPC[replica][opCode]
	if !ok {
		c.delayedRPC[replica][opCode] = make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE)
	}
	c.delayRPC[replica][opCode] = true
}

func (c *AbstractClient) ShouldDelayNextRPC(replica int, opCode uint8) bool {
	delay, ok := c.delayRPC[replica][opCode]
	c.delayRPC[replica][opCode] = false
	return ok && delay
}

func (c *AbstractClient) leaderListener(leader int) {
	var msgType byte
	var err error
	var errS string
	for !c.shutdown && err == nil {
		if msgType, err = c.leaders[leader].Reader.ReadByte(); err != nil {
			dlog.Printf("&&&&&&&&&&&&&&&&&Got this error from leader %d at time %v: %v\n", leader, time.Now().UnixMilli(), err)
			errS = "reading opcode"
			break
		}

		if rpair, present := c.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(c.leaders[leader].Reader); err != nil {
				errS = "unmarshling message"
				break
			}
			rpair.Chan <- obj
		} else {
			log.Printf("Error: received unknown message type: %d\n", msgType)
		}
	}
	if err != nil && err != io.EOF {
		log.Printf("Error %s from replica %d: %v\n", errS, leader, err)
		c.leaderAlive[leader] = false
	}
}

func (c *AbstractClient) replicaListener(i int, j int) {
	var msgType byte
	var err error
	var errS string
	for !c.shutdown && err == nil {
		if msgType, err = c.replicaConns[i][j].Reader.ReadByte(); err != nil {
			dlog.Printf("&&&&&&&&&&&&&&&&&Got this error from replica %d at time %v: %v\n", c.replicasPerShard[i][j], time.Now().UnixMilli(), err)
			errS = "reading opcode"
			break
		}

		if rpair, present := c.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(c.replicaConns[i][j].Reader); err != nil {
				errS = "unmarshling message"
				break
			}
			rpair.Chan <- obj
		} else {
			log.Printf("[replicaListener] Unknown message type %d (expected %d for PingReply)\n", msgType, clientproto.GEN_PING_REPLY)
			log.Printf("B Error: received unknown message type: %d\n", msgType)
		}
	}
	if err != nil && err != io.EOF {
		log.Printf("Error %s from replica %d: %v\n", errS, c.replicasPerShard[i][j], err)
		c.replicaAlive[i][j] = false
	}
}
