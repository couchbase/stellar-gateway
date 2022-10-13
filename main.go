package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/couchbase/stellar-nebula/common/clustering"
	"github.com/couchbase/stellar-nebula/common/psclustering"
	"github.com/couchbase/stellar-nebula/legacysystem"
	"github.com/couchbase/stellar-nebula/psimpl"
	"github.com/couchbase/stellar-nebula/pssystem"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/couchbase/gocb/v2"
	etcd "go.etcd.io/etcd/client/v3"
)

var cbHost = flag.String("cb-host", "couchbase://127.0.0.1", "the couchbase cluster to link to")
var cbUser = flag.String("cb-user", "Administrator", "the username to use for the couchbase cluster")
var cbPass = flag.String("cb-pass", "password", "the password to use for the couchbase cluster")
var etcdHost = flag.String("etcd-host", "localhost:2379", "the etcd host to connect to")
var bindAddr = flag.String("bind-addr", "0.0.0.0", "the address to bind")
var bindPort = flag.Int("bind-port", 18098, "the port to bind to")
var advertiseAddr = flag.String("advertise-addr", "127.0.0.1", "the address to use when advertising this node")
var advertisePort = flag.Uint64("advertise-port", 18098, "the port to use when advertising this node")
var nodeID = flag.String("node-id", "", "the local node id for this service")
var serverGroup = flag.String("server-group", "", "the local hostname for this service")
var verbose = flag.Bool("verbose", false, "whether to enable debug logging")

func main() {
	flag.Parse()

	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	if nodeID == nil || *nodeID == "" {
		genNodeID := uuid.NewString()
		nodeID = &genNodeID
	}

	// initialize the logger
	logLevel := zap.NewAtomicLevel()
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	fileEncoder := zapcore.NewJSONEncoder(config)
	consoleEncoder := zapcore.NewConsoleEncoder(config)
	logFile, _ := os.OpenFile("stellar-nebula.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	writer := zapcore.AddSync(logFile)
	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, logLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// switch to debug level logs for ... debugging
	if *verbose {
		logLevel.SetLevel(zap.DebugLevel)
	}

	// start connecting to the underlying cluster
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

	clusteringProvider, err := clustering.NewEtcdProvider(clustering.EtcdProviderOptions{
		EtcdClient: etcdClient,
		KeyPrefix:  "/nebula/topology",
	})
	if err != nil {
		log.Printf("failed to initialize clustering provider: %s", err)
		os.Exit(1)
	}

	clusteringManager := &psclustering.Manager{Provider: clusteringProvider}

	// join the cluster topology
	log.Printf("joining nebula cluster toplogy")
	clusteringManager.Join(context.Background(), &psclustering.Member{
		MemberID:      *nodeID,
		AdvertiseAddr: *advertiseAddr,
		AdvertisePort: int(*advertisePort),
		ServerGroup:   *serverGroup,
	})

	// setup the gateway server
	log.Printf("initializing gateway implementation")
	psImpl, err := psimpl.NewGateway(&psimpl.GatewayOptions{
		Logger:            logger,
		ClusteringManager: clusteringManager,
		CbClient:          client,
	})
	if err != nil {
		log.Fatalf("failed to initialize gateway implementation: %s", err)
	}

	log.Printf("initializing ps system")
	psSys, err := pssystem.NewSystem(&pssystem.SystemOptions{
		Logger:      logger,
		BindAddress: *bindAddr,
		BindPort:    *bindPort,
		Impl:        psImpl,
	})
	if err != nil {
		log.Fatalf("failed to initialize protostellar server: %s", err)
	}

	log.Printf("initializing legacy system")
	legacySys, err := legacysystem.NewSystem(&legacysystem.SystemOptions{
		Logger: logger,

		BindAddress: "",
		BindPorts: legacysystem.ServicePorts{
			Mgmt: 8091,
			Kv:   11210,
		},
		TLSBindPorts: legacysystem.ServicePorts{},

		DataServer:    psImpl.DataV1(),
		QueryServer:   psImpl.QueryV1(),
		RoutingServer: psImpl.RoutingV1(),
	})
	if err != nil {
		log.Printf("error creating legacy proxy: %s", err)
	}

	waitCh := make(chan struct{})

	go func() {
		// start serving requests
		log.Printf("starting to run ps system")
		err := psSys.Run(context.Background())
		if err != nil {
			log.Fatalf("failed to run ps system: %v", err)
		}

		waitCh <- struct{}{}
	}()

	go func() {
		// start serving requests
		log.Printf("starting to run legacy system")
		err := legacySys.Run(context.Background())
		if err != nil {
			log.Fatalf("failed to run legacy system: %v", err)
		}

		waitCh <- struct{}{}
	}()

	<-waitCh
	<-waitCh
}
