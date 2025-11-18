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
	leaders            []net.Conn
	readers            []*bufio.Reader
	writers            []*bufio.Writer
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
	replicaConns       [][]net.Conn
	replicaReaders     [][]*bufio.Reader
	replicaWriters     [][]*bufio.Writer
	replicaRetries     [][]int
	replicaAlive       [][]bool
}

func NewAbstractClient(id int32, coordinatorAddr string, coordinatorPort int, forceLeader int, statsFile string) *AbstractClient {
	c := &AbstractClient{
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
		make([]net.Conn, 0),           // replicas
		make([]*bufio.Reader, 0),      // readers
		make([]*bufio.Writer, 0),      // writers
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
		make([][]net.Conn, 0),           // replicaConns
		make([][]*bufio.Reader, 0),      // replicaReaders
		make([][]*bufio.Writer, 0),      // replicaWriters
		make([][]int, 0),                // replicaRetries
		make([][]bool, 0),               // replicasAlive
	}
	c.RegisterRPC(new(clientproto.PingReply), clientproto.GEN_PING_REPLY, c.pingReplyChan)

	c.ConnectToCoordinator()
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

func (c *AbstractClient) ConnectToCoordinator() {
	log.Printf("Dialing coordinator at addr %s:%d\n", c.coordinatorAddr, c.coordinatorPort)
	//////////////////////////////////////
	// Get info from coordinator
	//////////////////////////////////////
	coordinator, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.coordinatorAddr, c.coordinatorPort))
	if err != nil {
		log.Fatalf("Error connecting to coordinator: %v\n", err)
	}

	// Get the shard leaders
	llReply := new(coordinatorproto.GetShardLeaderListReply)
	err = coordinator.Call("Coordinator.GetShardLeaderList", new(coordinatorproto.GetShardLeaderListArgs), llReply)
	if err != nil {
		log.Fatalf("Error making the GetShardLeaderList RPC: %v\n", err)
	}
	log.Printf("Got shard leader list from coordinator: [")
	for i := 0; i < len(llReply.LeaderList); i++ {
		log.Printf("%s", llReply.LeaderList[i])
		if i != len(llReply.LeaderList)-1 {
			log.Printf(", ")
		}
	}
	log.Printf("]\n")

	// Get all replicas per shard
	srReply := new(coordinatorproto.GetReplicaListReply)
	err = coordinator.Call("Coordinator.GetReplicaList", new(coordinatorproto.GetReplicaListArgs), srReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC: %v\n", err)
	}
	log.Printf("Got replica list per shard from coordinator: [[")
	for i := 0; i < len(srReply.ReplicaListPerShard); i++ {
		for j := 0; j < len(srReply.ReplicaListPerShard[i]); j++ {
			log.Printf("%s", srReply.ReplicaListPerShard[i][j])
			if i != len(srReply.ReplicaListPerShard[i])-1 {
				log.Printf(", ")
			} else {
				log.Printf("], [")
			}
		}
		
	}
	log.Printf("]]\n")

	// TODO: Update coordinator to return leader and replicas for each shard.
	//       Then update data structures here to keep track of state for all of them.
	c.leaderAddrs = llReply.LeaderList
	c.numLeaders = len(c.leaderAddrs)
	c.leaderAlive = make([]bool, c.numLeaders)
	c.replicaPing = make([][]uint64, c.numLeaders)
	c.replicasByPingRank = make([][]int32, c.numLeaders)
	c.retries = make([]int, c.numLeaders)
	c.replicasPerShard = srReply.ReplicaListPerShard
	// c.delayRPC = make([]map[uint8]bool, c.numLeaders)
	// c.delayedRPC = make([]map[uint8]chan fastrpc.Serializable, c.numLeaders)
	// for i := 0; i < c.numLeaders; i++ {
	// 	c.delayRPC[i] = make(map[uint8]bool)
	// 	c.delayedRPC[i] = make(map[uint8]chan fastrpc.Serializable)
	// }
}

func (c *AbstractClient) ConnectToShards() {
	log.Printf("Connecting to shards...\n")
	c.leaders = make([]net.Conn, c.numLeaders)
	c.readers = make([]*bufio.Reader, c.numLeaders)
	c.writers = make([]*bufio.Writer, c.numLeaders)
	for i := 0; i < c.numLeaders; i++ {
		if !c.connectToLeader(i) {
			log.Fatalf("Must connect to all leaders on startup.\n")
		}
	}
	log.Printf("Successfully connected to all %d leaders.\n", c.numLeaders)
}

func (c *AbstractClient) ConnectToReplicas() {
	log.Printf("Connecting to replicas...\n")
	c.replicaConns = make([][]net.Conn, c.numLeaders)
	for i := range(c.replicaConns){
		c.replicaConns[i] = make([]net.Conn, len(c.replicasPerShard[i]))
	}
	c.replicaReaders = make([][]*bufio.Reader, c.numLeaders)
	for i := range(c.replicaReaders){
		c.replicaReaders[i] = make([]*bufio.Reader, len(c.replicasPerShard[i]))
	}
	c.replicaWriters = make([][]*bufio.Writer, c.numLeaders)
	for i := range(c.replicaWriters){
		c.replicaWriters[i] = make([]*bufio.Writer, len(c.replicasPerShard[i]))
	}
	c.replicaRetries = make([][]int, c.numLeaders)
	for i := range(c.replicaRetries){
		c.replicaRetries[i] = make([]int, len(c.replicasPerShard[i]))
	}
	c.replicaAlive = make([][]bool, c.numLeaders)
	for i := range(c.replicaAlive){
		c.replicaAlive[i] = make([]bool, len(c.replicasPerShard[i]))
	}
	// Add existing shard leader connections
	for i := 0; i < c.numLeaders; i++ {
		c.replicaConns[i][0] = c.leaders[i]
		c.replicaReaders[i][0] = c.readers[i]
		c.replicaWriters[i][0] = c.writers[i]
	}

	// Create new connections for rest of replicas
	for i := 0; i < len(c.replicasPerShard); i++ {
		for j := 1; j < len(c.replicasPerShard[i]); j++ {
			if !c.connectToReplica(i, j) {
				log.Fatalf("Must connect to all shard replicas on startup.\n")
			}
		}
		
	}
	log.Printf("Successfully connected to all replicas.\n")
}

func (c *AbstractClient) connectToReplica(i int, j int) bool {
	var err error
	if c.replicaConns[i][j] != nil {
		c.replicaConns[i][j].Close()
	}
	c.replicaRetries[i][j]++
	log.Printf("Dialing shard %d replica %d with addr %s\n", i, j, c.replicasPerShard[i][j])
	c.replicaConns[i][j], err = net.Dial("tcp", c.replicasPerShard[i][j])
	if err != nil {
		log.Printf("Error connecting to replica %d: %v\n", c.replicasPerShard[i][j], err)
		return false
	}
	log.Printf("Connected to shard %d replica %d with connection %s\n", i, j, c.replicasPerShard[i][j])
	c.replicaReaders[i][j] = bufio.NewReader(c.replicaConns[i][j])
	c.replicaWriters[i][j] = bufio.NewWriter(c.replicaConns[i][j])

	var idBytes [4]byte
	idBytesS := idBytes[:4]
	binary.LittleEndian.PutUint32(idBytesS, uint32(c.id))
	c.replicaWriters[i][j].Write(idBytesS)
	c.replicaWriters[i][j].Flush()

	c.replicaAlive[i][j] = true
	go c.replicaListener(i,j)
	return true
}

func (c *AbstractClient) connectToLeader(i int) bool {
	var err error
	if c.leaders[i] != nil {
		c.leaders[i].Close()
	}
	c.retries[i]++
	log.Printf("Dialing leader %d with addr %s\n", i, c.leaderAddrs[i])
	c.leaders[i], err = net.Dial("tcp", c.leaderAddrs[i])
	if err != nil {
		log.Printf("Error connecting to leader %d: %v\n", i, err)
		return false
	}
	log.Printf("Connected to leader %d with connection %s\n", i, c.leaders[i].LocalAddr().String())
	c.readers[i] = bufio.NewReader(c.leaders[i])
	c.writers[i] = bufio.NewWriter(c.leaders[i])

	var idBytes [4]byte
	idBytesS := idBytes[:4]
	binary.LittleEndian.PutUint32(idBytesS, uint32(c.id))
	c.writers[i].Write(idBytesS)
	c.writers[i].Flush()

	c.leaderAlive[i] = true
	go c.leaderListener(i)
	return true
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
	err = c.replicaWriters[i][j].WriteByte(clientproto.GEN_PING)
	log.Printf("Wrote GEN_PING opcode to replica %s\n", c.replicasPerShard[i][j])
	if err != nil {
		log.Printf("Error writing GEN_PING opcode to replica %d: %v\n", c.replicasPerShard[i][j], err)
		done <- false
		return
	}
	ping := &clientproto.Ping{c.id, uint64(time.Now().UnixNano())}
	ping.Marshal(c.replicaWriters[i][j])
	err = c.replicaWriters[i][j].Flush()
	log.Printf("Flushed ping to replica %d\n", c.replicasPerShard[i][j])
	if err != nil {
		log.Printf("Error flushing connection to replica %d: %v\n", c.replicasPerShard[i][j], err)
		done <- false
		return
	}
	select {
	case pingReplyS := <-c.pingReplyChan:
		pingReply := pingReplyS.(*clientproto.PingReply)
		c.replicaPing[0][pingReply.ReplicaId] = uint64(time.Now().UnixNano()) -
			pingReply.Ts
		log.Printf("Received ping from shard %d replica %d in time %d\n",
			0, pingReply.ReplicaId, c.replicaPing[0][pingReply.ReplicaId])
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
		if msgType, err = c.readers[leader].ReadByte(); err != nil {
			dlog.Printf("&&&&&&&&&&&&&&&&&Got this error from leader %d at time %v: %v\n", leader, time.Now().UnixMilli(), err)
			errS = "reading opcode"
			break
		}

		if rpair, present := c.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(c.readers[leader]); err != nil {
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
		if msgType, err = c.replicaReaders[i][j].ReadByte(); err != nil {
			dlog.Printf("&&&&&&&&&&&&&&&&&Got this error from replica %d at time %v: %v\n", c.replicasPerShard[i][j], time.Now().UnixMilli(), err)
			errS = "reading opcode"
			break
		}

		if rpair, present := c.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(c.replicaReaders[i][j]); err != nil {
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
