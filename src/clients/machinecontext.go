package clients

import (
	"bufio"
	"coordinatorproto"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type MachineContext struct {
    Config    *ClusterConfig
    Transport *Transport
}

type ClusterConfig struct {
	LeaderAddrs          []string
	ReplicaAddrsPerShard [][]string
}

type SharedConn struct {
    Conn   net.Conn
    Reader *bufio.Reader
    Writer *bufio.Writer
}

type Transport struct {
    mu    sync.Mutex
    conns map[string]*SharedConn // key = replica address
}

var (
	globalConfig *ClusterConfig
	configOnce   sync.Once
	configErr    error
)

func GetClusterConfig(coordinatorAddr string, coordinatorPort int) (*ClusterConfig, error) {
	configOnce.Do(func() {
		globalConfig, configErr = fetchClusterConfig(coordinatorAddr, coordinatorPort)
	})
	return globalConfig, configErr
}

func fetchClusterConfig(coordinatorAddr string, coordinatorPort int) (*ClusterConfig, error) {
	addr := fmt.Sprintf("%s:%d", coordinatorAddr, coordinatorPort)

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// ---- Coordinator RPCs ----

	// Get the shard leaders
	llReply := new(coordinatorproto.GetShardLeaderListReply)
	if err := client.Call(
		"Coordinator.GetShardLeaderList",
		new(coordinatorproto.GetShardLeaderListArgs),
		llReply,
	); err != nil {
		return nil, err
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
	if err := client.Call(
		"Coordinator.GetReplicaList",
		new(coordinatorproto.GetReplicaListArgs),
		srReply,
	); err != nil {
		return nil, err
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

	return &ClusterConfig{
		LeaderAddrs:          llReply.LeaderList,
		ReplicaAddrsPerShard: srReply.ReplicaListPerShard
	}, nil
}

func NewTransport() *Transport {
    return &Transport{
        conns: make(map[string]*SharedConn),
    }
}

func (t *Transport) ConnectIfAbsent(addr string) (*SharedConn, error) {
    return t.Get(addr)
}

func (t *Transport) Get(addr string) (*SharedConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Reuse connection if already existing
	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}

	// Otherwise create new TCP connection
	netConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	shared := &SharedConn{
		Conn:   netConn,
		Reader: bufio.NewReader(netConn),
		Writer: bufio.NewWriter(netConn),
	}

	t.conns[addr] = shared
	return shared, nil
}