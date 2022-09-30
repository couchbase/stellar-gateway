package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/couchbase/stellar-nebula/common/topology"
	"github.com/couchbase/stellar-nebula/gateway/server_v1"
	"github.com/google/uuid"

	"github.com/couchbase/gocb/v2"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var cbHost = flag.String("cb-host", "couchbase://127.0.0.1", "the couchbase cluster to link to")
var cbUser = flag.String("cb-user", "Administrator", "the username to use for the couchbase cluster")
var cbPass = flag.String("cb-pass", "password", "the password to use for the couchbase cluster")
var etcdHost = flag.String("etcd-host", "localhost:2379", "the etcd host to connect to")
var bindAddr = flag.String("bind-addr", "0.0.0.0", "the address to bind")
var bindPort = flag.Uint64("bind-port", 18098, "the port to bind to")
var advertiseAddr = flag.String("advertise-addr", "127.0.0.1", "the address to use when advertising this node")
var advertisePort = flag.Uint64("advertise-port", 18098, "the port to use when advertising this node")
var nodeID = flag.String("node-id", "", "the local node id for this service")
var serverGroup = flag.String("server-group", "", "the local hostname for this service")

func main() {
	flag.Parse()

	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	if nodeID == nil || *nodeID == "" {
		genNodeID := uuid.NewString()
		nodeID = &genNodeID
	}

	log.Printf("linking to couchbase cluster at: %s (user: %s)", *cbHost, *cbUser)

	client, err := gocb.Connect(*cbHost, gocb.ClusterOptions{
		Username: *cbUser,
		Password: *cbPass,
	})
	if err != nil {
		log.Printf("failed to connect to couchbase cluster: %s", err)
		os.Exit(1)
	}

	err = client.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		log.Printf("failed to wait for couchbase cluster connection: %s", err)
		os.Exit(1)
	}

	log.Printf("connected to couchbase cluster")

	log.Printf("connect to etcd instance at: %s", *etcdHost)

	etcdClient, err := etcd.New(etcd.Config{
		Endpoints:   []string{*etcdHost},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Printf("failed to connect to etcd: %s", err)
		os.Exit(1)
	}

	topologyProvider, err := topology.NewEtcdProvider(topology.EtcdProviderOptions{
		EtcdClient: etcdClient,
		KeyPrefix:  "/nebula/topology",
	})
	if err != nil {
		log.Printf("failed to initialize topology provider: %s", err)
		os.Exit(1)
	}

	// setup the grpc server
	log.Printf("initializing grpc system")
	s := grpc.NewServer()
	server_v1.Register(s, topologyProvider, client)

	// start our listener for grpc
	log.Printf("initializing grpc listener")
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *bindAddr, *bindPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("grpc listener is listening at %v", lis.Addr())

	// join the cluster topology
	log.Printf("joining nebula cluster toplogy")
	topologyProvider.Join(&topology.Endpoint{
		NodeID:        *nodeID,
		AdvertiseAddr: *advertiseAddr,
		AdvertisePort: int(*advertisePort),
		ServerGroup:   *serverGroup,
	})

	// start serving requests
	log.Printf("initialization complete")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
