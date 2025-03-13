/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/proxy"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/ratelimiting"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/gateway/system"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	cbconfigx "github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type ServicePorts struct {
	PS   int `json:"p,omitempty"`
	SD   int `json:"s,omitempty"`
	DAPI int `json:"d,omitempty"`
}

type StartupInfo struct {
	MemberID       string
	ServerGroup    string
	AdvertiseAddr  string
	AdvertisePorts ServicePorts
}

type Config struct {
	Logger         *zap.Logger
	NodeID         string
	ServerGroup    string
	Daemon         bool
	Debug          bool
	ProxyServices  []string
	AlphaEndpoints bool

	CbConnStr string
	Username  string
	Password  string

	BindAddress      string
	BindDataPort     int
	BindSdPort       int
	BindDapiPort     int
	AdvertiseAddress string
	AdvertisePorts   ServicePorts

	RateLimit int

	GrpcCertificate tls.Certificate
	DapiCertificate tls.Certificate

	NumInstances    uint
	StartupCallback func(*StartupInfo)
}

type Gateway struct {
	config Config

	isShutdown     atomic.Bool
	shutdownSig    chan struct{}
	atomicGrpcCert atomic.Pointer[tls.Certificate]
	atomicDapiCert atomic.Pointer[tls.Certificate]

	reconfigureLock sync.Mutex
	rateLimiters    []*ratelimiting.GlobalRateLimiter
}

func NewGateway(config *Config) (*Gateway, error) {
	gw := &Gateway{
		config:      *config,
		shutdownSig: make(chan struct{}),
	}

	grpcCert := config.GrpcCertificate
	gw.atomicGrpcCert.Store(&grpcCert)

	dapiCert := config.DapiCertificate
	gw.atomicDapiCert.Store(&dapiCert)

	return gw, nil
}

func connStrToMgmtHostPort(connStr string) (string, error) {
	// attempt to parse the connection string
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return "", err
	}

	// if the connection string is blank, assume http
	if connSpec.Scheme == "" {
		connSpec.Scheme = "http"
	}

	// we use cbauth, and thus can only bootstrap with http
	if connSpec.Scheme != "http" {
		return "", errors.New("only the http connection string scheme is supported")
	}

	// we only support a single host to bootstrap with
	if len(connSpec.Addresses) != 1 {
		return "", errors.New("you must pass exactly one address in the connection string")
	}

	// grab the address
	address := connSpec.Addresses[0]

	// if the port is undefined, assume its 8091
	if address.Port == -1 {
		address.Port = 8091
	}

	// calculate the full host/port pair
	hostPort := fmt.Sprintf("%s:%d", address.Host, address.Port)

	return hostPort, nil
}

func pingCouchbaseCluster(
	ctx context.Context,
	mgmtHostPort,
	username, password string,
) (string, error) {
	mgmt := &cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "cloud-native-gateway-startup",
		Endpoint:  "http://" + mgmtHostPort,
		Username:  username,
		Password:  password,
	}

	clusterConfig, err := mgmt.GetClusterConfig(ctx, &cbmgmtx.GetClusterConfigOptions{})
	if err != nil {
		return "", errors.Wrap(err, "failed to get cluster config")
	}

	var thisNode *cbconfigx.FullNodeJson
	for _, node := range clusterConfig.Nodes {
		if node.ThisNode {
			thisNode = &node
		}
	}

	if thisNode == nil {
		return "", errors.New("failed to find local bootstrap node in node list")
	}

	if thisNode.ClusterMembership != "active" {
		return "", errors.New("bootstrap node is not part of a cluster yet")
	}

	clusterInfo, err := mgmt.GetTerseClusterConfig(ctx, &cbmgmtx.GetTerseClusterConfigOptions{})
	if err != nil {
		return "", errors.Wrap(err, "failed to get cluster info")
	}

	return clusterInfo.UUID, nil
}

func (g *Gateway) Run(ctx context.Context) error {
	config := g.config

	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	// start connecting to the underlying cluster
	config.Logger.Info("linking to couchbase cluster", zap.String("connectionString", config.CbConnStr), zap.String("User", config.Username))

	// identify the ns_server host/port
	mgmtHostPort, err := connStrToMgmtHostPort(config.CbConnStr)
	if err != nil {
		config.Logger.Error("failed to parse connection string", zap.Error(err))
		return err
	}
	config.Logger.Info("identified couchbase server address", zap.String("address", mgmtHostPort))

	// ping the cluster first to make sure its alive
	config.Logger.Info("waiting for couchbase server to become available", zap.String("address", mgmtHostPort))

	var clusterUUID string
	for {
		currentUUID, err := pingCouchbaseCluster(ctx, mgmtHostPort, config.Username, config.Password)
		if err != nil {
			config.Logger.Warn("failed to ping cluster", zap.Error(err))

			// if we are not in daemon mode, we just immediately return the error to the user
			if !config.Daemon {
				return err
			}

			// wait before we retry
			waitTime := 5 * time.Second
			config.Logger.Info("sleeping before trying to ping cluster again", zap.Duration("period", waitTime))
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}

			continue
		}

		// once we successfully ping the cluster, we can stop polling
		clusterUUID = currentUUID
		break
	}

	// initialize cb-auth
	authenticator, err := auth.NewCbAuthAuthenticator(ctx, auth.NewCbAuthAuthenticatorOptions{
		NodeId:      nodeID,
		Addresses:   []string{mgmtHostPort},
		Username:    config.Username,
		Password:    config.Password,
		ClusterUUID: clusterUUID,
		Logger:      config.Logger.Named("cbauth"),
	})
	if err != nil {
		config.Logger.Error("failed to initialize cbauth connection",
			zap.Error(err),
			zap.String("hostPort", mgmtHostPort),
			zap.String("user", config.Username))
		return err
	}

	// try to establish a client connection to the cluster
	agentMgr, err := gocbcorex.CreateBucketsTrackingAgentManager(ctx, gocbcorex.BucketsTrackingAgentManagerOptions{
		Logger:    config.Logger.Named("gocbcorex"),
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: []string{mgmtHostPort},
		},
	})
	if err != nil {
		config.Logger.Error("failed to connect to couchbase cluster",
			zap.Error(err),
			zap.String("httpAddr", mgmtHostPort),
			zap.String("user", config.Username))
		return err
	}

	config.Logger.Info("connected agent manager",
		zap.Any("agent", agentMgr))

	config.Logger.Info("connected to couchbase cluster")

	// TODO(brett19): We should use the gocb client to fetch the topologies.
	cbTopologyProvider, err := cbtopology.NewPollingProvider(cbtopology.PollingProviderOptions{
		Fetcher: cbconfig.NewFetcher(cbconfig.FetcherOptions{
			Host:     config.CbConnStr,
			Username: config.Username,
			Password: config.Password,
			Logger:   config.Logger.Named("fetcher"),
		}),
		Logger: config.Logger.Named("topology-provider"),
	})
	if err != nil {
		config.Logger.Error("failed to initialize cb topology poller")
		return err
	}

	goclusteringProvider, err := goclustering.NewInProcProvider(goclustering.InProcProviderOptions{})
	if err != nil {
		config.Logger.Error("failed to initialize in-proc clustering provider")
		return err
	}

	clusteringManager := &clustering.Manager{
		Provider: goclusteringProvider,
		Logger:   config.Logger.Named("clustering-manager"),
	}

	psTopologyManager, err := topology.NewManager(&topology.ManagerOptions{
		LocalTopologyProvider:  clusteringManager,
		RemoteTopologyProvider: cbTopologyProvider,
		Logger:                 config.Logger.Named("topology-manager"),
	})
	if err != nil {
		config.Logger.Error("failed to initialize topology manager")
		return err
	}

	var proxyServices []proxy.ServiceType
	for _, serviceName := range config.ProxyServices {
		proxyServices = append(proxyServices, proxy.ServiceType(serviceName))
	}

	go func() {
		watchCh := agentMgr.WatchConfig(context.Background())
	runLoop:
		for {
			select {
			case <-g.shutdownSig:
				break runLoop
			case cfg := <-watchCh:
				if cfg == nil {
					continue
				}

				mgmtEndpointsList := make([]string, 0, len(cfg.Nodes))
				for _, node := range cfg.Nodes {
					if node.Addresses.NonSSLPorts.Mgmt > 0 {
						mgmtEndpointsList = append(mgmtEndpointsList,
							fmt.Sprintf("%s:%d", node.Addresses.Hostname, node.Addresses.NonSSLPorts.Mgmt))
					}
				}

				err := authenticator.Reconfigure(auth.CbAuthAuthenticatorReconfigureOptions{
					Addresses:   mgmtEndpointsList,
					Username:    config.Username,
					Password:    config.Password,
					ClusterUUID: clusterUUID,
				})
				if err != nil {
					config.Logger.Warn("failed to reconfigure cbauth",
						zap.Error(err))
				}
			}
		}

		err := authenticator.Close()
		if err != nil {
			config.Logger.Warn("failed to shutdown cbauth",
				zap.Error(err))
		}
	}()

	startInstance := func(ctx context.Context, instanceIdx int) error {
		rateLimiter := ratelimiting.NewGlobalRateLimiter(uint64(config.RateLimit), time.Second)

		dataImpl := dataimpl.New(&dataimpl.NewOptions{
			Logger:           config.Logger.Named("data-impl"),
			Debug:            config.Debug,
			TopologyProvider: psTopologyManager,
			CbClient:         agentMgr,
			Authenticator:    authenticator,
		})

		sdImpl := sdimpl.New(&sdimpl.NewOptions{
			Logger:           config.Logger.Named("sd-impl"),
			TopologyProvider: psTopologyManager,
		})

		dapiImpl := dapiimpl.New(&dapiimpl.NewOptions{
			Logger:        config.Logger.Named("dapi-impl"),
			Debug:         config.Debug,
			CbClient:      agentMgr,
			Authenticator: authenticator,
			ProxyServices: proxyServices,
		})

		config.Logger.Info("initializing protostellar system")
		gatewaySys, err := system.NewSystem(&system.SystemOptions{
			Logger:      config.Logger.Named("gateway-system"),
			DataImpl:    dataImpl,
			SdImpl:      sdImpl,
			DapiImpl:    dapiImpl,
			Metrics:     metrics.GetSnMetrics(),
			RateLimiter: rateLimiter,
			GrpcTlsConfig: &tls.Config{
				GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return g.atomicGrpcCert.Load(), nil
				},
			},
			DapiTlsConfig: &tls.Config{
				GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return g.atomicDapiCert.Load(), nil
				},
			},
			AlphaEndpoints: config.AlphaEndpoints,
			Debug:          config.Debug,
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy")
			return err
		}

		dataPort := config.BindDataPort
		sdPort := config.BindSdPort
		dapiPort := config.BindDapiPort

		// the non-0 instance uses randomized ports
		if instanceIdx > 0 {
			dataPort = 0
			sdPort = 0
			dapiPort = 0
		}

		gatewayLis, err := system.NewListeners(&system.ListenersOptions{
			Address:  config.BindAddress,
			DataPort: dataPort,
			SdPort:   sdPort,
			DapiPort: dapiPort,
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy listeners")
			return err
		}

		advertiseAddr := config.AdvertiseAddress
		if advertiseAddr == "" {
			advertiseAddr, err = netutils.GetAdvertiseAddress(config.BindAddress)
			if err != nil {
				config.Logger.Error("failed to identify advertise address")
				return err
			}
		}

		pickPort := func(advertisePort int, boundPort int) int {
			if advertisePort != 0 {
				return advertisePort
			}
			return boundPort
		}
		advertisePorts := clustering.ServicePorts{
			PS:   pickPort(config.AdvertisePorts.PS, gatewayLis.BoundDataPort()),
			SD:   pickPort(config.AdvertisePorts.SD, gatewayLis.BoundSdPort()),
			DAPI: pickPort(config.AdvertisePorts.DAPI, gatewayLis.BoundDapiPort()),
		}

		localMemberData := &clustering.Member{
			MemberID:       nodeID,
			ServerGroup:    serverGroup,
			AdvertiseAddr:  advertiseAddr,
			AdvertisePorts: advertisePorts,
		}

		clusterEntry, err := clusteringManager.Join(ctx, localMemberData)
		if err != nil {
			config.Logger.Error("failed to join cluster")
			return err
		}

		go func() {
			<-g.shutdownSig
			gatewaySys.Shutdown()
		}()

		g.reconfigureLock.Lock()
		g.rateLimiters = append(g.rateLimiters, rateLimiter)
		g.reconfigureLock.Unlock()

		config.Logger.Info("starting to run protostellar system",
			zap.Int("advertisedPortPS", advertisePorts.PS),
			zap.Int("advertisedPortSD", advertisePorts.SD),
			zap.Int("advertisedPortDAPI", advertisePorts.DAPI))

		if instanceIdx == 0 && config.StartupCallback != nil {
			config.StartupCallback(&StartupInfo{
				MemberID:      nodeID,
				ServerGroup:   serverGroup,
				AdvertiseAddr: advertiseAddr,
				AdvertisePorts: ServicePorts{
					PS:   advertisePorts.PS,
					SD:   advertisePorts.SD,
					DAPI: advertisePorts.DAPI,
				},
			})
		}

		log.Printf("Starting to serve...")

		err = gatewaySys.Serve(ctx, gatewayLis)
		if err != nil {
			config.Logger.Error("failed to serve protostellar system")

			leaveErr := clusterEntry.Leave(ctx)
			if leaveErr != nil {
				config.Logger.Error("failed to leave cluster")
			}

			return err
		}

		log.Printf("Gateway instance %d has shut down", instanceIdx)

		err = gatewayLis.Close()
		if err != nil {
			config.Logger.Error("failed to close listener")
			return err
		}

		err = clusterEntry.Leave(ctx)
		if err != nil {
			config.Logger.Error("failed to leave cluster")
			return err
		}

		if g.isShutdown.CompareAndSwap(false, true) {
			close(g.shutdownSig)
		}

		return nil
	}

	errCh := make(chan error)
	for instanceIdx := 0; instanceIdx < int(config.NumInstances); instanceIdx++ {
		go func(instanceIdx int) {
			errCh <- startInstance(ctx, instanceIdx)
		}(instanceIdx)
	}

	for instanceIdx := 0; instanceIdx < int(config.NumInstances); instanceIdx++ {
		err := <-errCh
		if err != nil {
			// we need to start a goroutine to read the rest of the finishes
			go func() {
				for i := instanceIdx + 1; i < int(config.NumInstances); i++ {
					<-errCh
				}
			}()

			return err
		}
	}

	return nil
}

type ReconfigureOptions struct {
	RateLimit int
}

func (g *Gateway) Reconfigure(opts *ReconfigureOptions) error {
	g.reconfigureLock.Lock()
	defer g.reconfigureLock.Unlock()

	for _, rateLimiter := range g.rateLimiters {
		rateLimiter.ResetAndUpdateRateLimit(uint64(opts.RateLimit), time.Second)
	}

	return nil
}

func (g *Gateway) Shutdown() {
	if g.isShutdown.CompareAndSwap(false, true) {
		close(g.shutdownSig)
	}
}
