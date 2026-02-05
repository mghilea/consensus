package main

import (
	"bufio"
	"clients"
	"dlog"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"state"
	"sync"
	"time"
  "zipfgenerator"
)

var clientId *int = flag.Int(
	"clientId",
	0,
	"Client identifier for use in replication protocols.")

var clientProcs *int = flag.Int(
	"clientProcs",
	1,
	"Number of client processes running on this client.")

var clientPoolSize *int = flag.Int(
	"clientPoolSize",
	1,
	"Number of physical clients this machine holds.")

var conflicts *int = flag.Int(
	"conflicts",
	-1,
	"Percentage of conflicts. If < 0, a zipfian distribution will be used for "+
		"choosing keys.")

var conflictsDenom *int = flag.Int(
	"conflictsDenom",
	100,
	"Denominator of conflict fraction when conflicts >= 0.")

var cpuProfile *string = flag.String(
	"cpuProfile",
	"",
	"Name of file for CPU profile. If empty, no profile is created.")

var debug *bool = flag.Bool(
	"debug",
	true,
	"Enable debug output.")

var defaultReplicaOrder *bool = flag.Bool(
	"defaultReplicaOrder",
	false,
	"Use default replica order for Gryff coordination.")

var epaxosMode *bool = flag.Bool(
	"epaxosMode",
	false,
	"Run Gryff with same message pattern as EPaxos.")

var expLength *int = flag.Int(
	"expLength",
	30,
	"Length of the timed experiment (in seconds).")

var fastPaxos *bool = flag.Bool(
	"fastPaxos",
	false,
	"Send message directly to all replicas a la Fast Paxos.")

var forceLeader *int = flag.Int(
	"forceLeader",
	-1,
	"Replica ID to which leader-based operations will be sent. If < 0, an "+
		"appropriate leader is chosen by default.")

var coordinatorAddr *string = flag.String(
	"caddr",
	"",
	"Coordinator address.")

var coordinatorPort *int = flag.Int(
	"cport",
	7097,
	"Coordinator port.")

var maxProcessors *int = flag.Int(
	"maxProcessors",
	0,
	"GOMAXPROCS. Defaults to 0")

var numKeys *uint64 = flag.Uint64(
	"numKeys",
	10000,
	"Number of keys in simulated store.")

var proxy *bool = flag.Bool(
	"proxy",
	false,
	"Proxy writes at local replica.")

var rampDown *int = flag.Int(
	"rampDown",
	5,
	"Length of the cool-down period after statistics are measured (in seconds).")

var rampUp *int = flag.Int(
	"rampUp",
	5,
	"Length of the warm-up period before statistics are measured (in seconds).")

var randSleep *int = flag.Int(
	"randSleep",
	0,
	"Max number of milliseconds to sleep after operation completed.")

var randomLeader *bool = flag.Bool(
	"randomLeader",
	false,
	"Egalitarian (no leader).")

var reads *int = flag.Int(
	"reads",
	0,
	"Percentage of reads.")

var regular *bool = flag.Bool(
	"regular",
	false,
	"Perform operations with regular consistency. (only for applicable protocols)")

var replProtocol *string = flag.String(
	"replProtocol",
	"",
	"Replication protocol used by clients and servers.")

var rmws *int = flag.Int(
	"rmws",
	0,
	"Percentage of rmws.")

var sequential *bool = flag.Bool(
	"sequential",
	true,
	"Perform operations with sequential consistency. "+
		"(only for applicable protocols")

var statsFile *string = flag.String(
	"statsFile",
	"",
	"Export location for collected statistics. If empty, no file file is written.")

var fanout *int = flag.Int(
	"fanout",
	1,
	"Fanout. Defaults to 1.")

var singleShardAware *bool = flag.Bool(
	"SSA",
	false,
	"Single shard awareness optimization. Defaults to false.")

var thrifty *bool = flag.Bool(
	"thrifty",
	false,
	"Only initially send messages to nearest quorum of replicas.")

var writes *int = flag.Int(
	"writes",
	1000,
	"Percentage of updates (writes).")

var zipfS = flag.Float64(
	"zipfS",
	2,
	"Zipfian s parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

var zipfV = flag.Float64(
	"zipfV",
	1,
	"Zipfian v parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

type Result struct {
	op   string
	lat  int64
	key  int32
	cnt  int32
}

type ClientJob struct {
	client clients.Client
	id     int32
}

func createClientWithID(uniqueID int32) clients.Client {
	switch *replProtocol {
	case "abd":
		return clients.NewAbdClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular)
	case "gryff":
		return clients.NewGryffClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular, *sequential, *proxy, *thrifty, *defaultReplicaOrder,
			*epaxosMode)
	case "epaxos":
		return clients.NewProposeClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true)
	case "mdl":
		return clients.NewMDLClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true, *singleShardAware)
	case "ss-mdl":
		return clients.NewSSMDLClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
	default:
		return clients.NewProposeClient(uniqueID, *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
	}
}

func Max(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func clientWorker(uniqueID int32, clientPool chan clients.Client, stop <-chan struct{}, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	r := rand.New(rand.NewSource(int64(uniqueID) + time.Now().UnixNano()))
	zipf, _ := zipfgenerator.NewZipfGenerator(r, 0, *numKeys, *zipfS, false)
	
	opTypes := make([]state.Operation, *fanout)
	keys := make([]int64, *fanout)
	count := int32(0)

	startTime := time.Now()
	rampUpTime := time.Duration(*rampUp) * time.Second
	expEndTime := time.Duration(*expLength-*rampDown) * time.Second

	for {
		select {
		case <-stop:
			endTime := time.Now()
			log.Printf("Client thread terminated after %.2f seconds. Total app requests completed: %d\n", endTime.Sub(startTime).Seconds(), count)
			return
		default:
		}

		if *randSleep > 0 {
			time.Sleep(time.Duration(r.Intn(*randSleep * 1e6)))
		}

		for j := 0; j < *fanout; j++ {
			roll := r.Intn(1000)
			if roll < *reads {
				opTypes[j] = state.GET
			} else if roll < *reads+*writes {
				opTypes[j] = state.PUT
			} else {
				opTypes[j] = state.CAS
			}
			keys[j] = int64(zipf.Uint64())
		}

		// Acquire client from pool
		c := <-clientPool

		before := time.Now()
		success, _ := c.AppRequest(opTypes, keys)
		after := time.Now()

		// Return client to the pool
		clientPool <- c
		
		elapsed := after.Sub(startTime)
		if elapsed >= rampUpTime && elapsed < expEndTime {
			if success { 
				count++
				results <- Result{"app", after.Sub(before).Nanoseconds(), uniqueID, count}
			}
		}
	}
}

func main() {
	flag.Parse()

	if *maxProcessors > 0 {
		runtime.GOMAXPROCS(*maxProcessors)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	if *conflicts >= 0 {
		dlog.Println("Using uniform distribution")
	} else {
		dlog.Println("Using zipfian distribution")
	}

	if *writes+*reads+*rmws != 1000 {
		log.Fatalf("Writes (%d), reads (%d), and rmws (%d) must add up to 1000.\n", *writes, *reads, *rmws)
	}
	
	stop := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan Result, 1000000)

	var loggerWg sync.WaitGroup
	loggerWg.Add(1)
	go func() {
		defer loggerWg.Done()
		writer := bufio.NewWriterSize(os.Stdout, 16*1024*1024)
		defer writer.Flush()

		for r := range results {
			fmt.Fprintf(writer, "%s,%d,%d,%d\n", r.op, r.lat, r.key, r.cnt)
		}
	}()

	// Create pool of clients (establishing connections to the servers)
	clientPool := make(chan clients.Client, *clientProcs)

	log.Printf("Initializing a pool of %d clients...\n", *clientProcs)
	initStart := time.Now()
    var initWg sync.WaitGroup
    for i := 0; i < *clientProcs; i++ {
        initWg.Add(1)
        go func(idx int) {
            defer initWg.Done()
            client := createClientWithID(int32(*clientId*100000 + idx))
            clientPool <- client
        }(i)
    }
    initWg.Wait()
	initEnd := time.Now()
	initTime := initEnd.Sub(initStart).Seconds()
    log.Printf("All %d clients initialized in %.2f seconds.", *clientProcs, initTime)

	nextID := int32(*clientId * 1000000)
	for i := 0; i < *clientProcs; i++ {
		wg.Add(1)
		go clientWorker(nextID, clientPool, stop, results, &wg)
		nextID++
	}

	// Run for expLength seconds
	time.Sleep(time.Duration(*expLength) * time.Second)
	close(stop)
	wg.Wait()
	close(results)
	loggerWg.Wait()
}