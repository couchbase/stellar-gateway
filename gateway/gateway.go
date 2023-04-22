package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/contrib/cbauthauth"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/gateway/system"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ServicePorts struct {
	PS int `json:"p,omitempty"`
	SD int `json:"s,omitempty"`
}

type StartupInfo struct {
	MemberID       string
	ServerGroup    string
	AdvertiseAddr  string
	AdvertisePorts ServicePorts
}

type Config struct {
	Logger      *zap.Logger
	NodeID      string
	ServerGroup string
	Daemon      bool

	CbConnStr string
	Username  string
	Password  string

	BindAddress      string
	BindDataPort     int
	BindSdPort       int
	AdvertiseAddress string
	AdvertisePorts   ServicePorts

	TlsCertificate tls.Certificate

	NumInstances    uint
	StartupCallback func(*StartupInfo)
}

type Gateway struct {
	config Config

	atomicTlsCert atomic.Pointer[tls.Certificate]
}

func NewGateway(config *Config) (*Gateway, error) {
	gw := &Gateway{
		config: *config,
	}

	tlsCert := config.TlsCertificate
	gw.atomicTlsCert.Store(&tlsCert)

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

func pingCouchbaseCluster(mgmtHostPort string) error {
	pollEndpoint := fmt.Sprintf("http://%s", mgmtHostPort)

	res, err := http.Get(pollEndpoint)
	if err != nil {
		return errors.Wrap(err, "failed to execute GET operation")
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	return nil
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

	for {
		err = pingCouchbaseCluster(mgmtHostPort)
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
		break
	}

	// initialize cb-auth
	_, err = cbauth.InternalRetryDefaultInitWithService("stg", mgmtHostPort, config.Username, config.Password)
	if err != nil {
		if strings.Contains(err.Error(), "already initialized") {
			// we ignore this error
		} else {
			config.Logger.Error("failed to initialize cbauth connection",
				zap.Error(err),
				zap.String("hostPort", mgmtHostPort),
				zap.String("user", config.Username))
			return err
		}
	}

	// try to establish a client connection to the cluster
	agentMgr, err := gocbcorex.CreateAgentManager(ctx, gocbcorex.AgentManagerOptions{
		Logger:        config.Logger.Named("gocbcorex"),
		TLSConfig:     nil,
		Authenticator: &cbauthauth.CbAuthAuthenticator{},
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

	startInstance := func(ctx context.Context, instanceIdx int) error {
		dataImpl := dataimpl.New(&dataimpl.NewOptions{
			Logger:           config.Logger.Named("data-impl"),
			TopologyProvider: psTopologyManager,
			CbClient:         agentMgr,
			Authenticator:    auth.CbAuthAuthenticator{},
		})

		sdImpl := sdimpl.New(&sdimpl.NewOptions{
			Logger:           config.Logger.Named("sd-impl"),
			TopologyProvider: psTopologyManager,
		})

		config.Logger.Info("initializing protostellar system")
		gatewaySys, err := system.NewSystem(&system.SystemOptions{
			Logger:   config.Logger.Named("gateway-system"),
			DataImpl: dataImpl,
			SdImpl:   sdImpl,
			Metrics:  metrics.GetSnMetrics(),
			TlsConfig: &tls.Config{
				GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return g.atomicTlsCert.Load(), nil
				},
			},
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy")
			return err
		}

		dataPort := config.BindDataPort
		sdPort := config.BindSdPort

		// the non-0 instance uses randomized ports
		if instanceIdx > 0 {
			dataPort = 0
			sdPort = 0
		}

		gatewayLis, err := system.NewListeners(&system.ListenersOptions{
			Address:  config.BindAddress,
			DataPort: dataPort,
			SdPort:   sdPort,
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
			PS: pickPort(config.AdvertisePorts.PS, gatewayLis.BoundDataPort()),
			SD: pickPort(config.AdvertisePorts.SD, gatewayLis.BoundSdPort()),
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

		if instanceIdx == 0 && config.StartupCallback != nil {
			config.StartupCallback(&StartupInfo{
				MemberID:      nodeID,
				ServerGroup:   serverGroup,
				AdvertiseAddr: advertiseAddr,
				AdvertisePorts: ServicePorts{
					PS: advertisePorts.PS,
					SD: advertisePorts.SD,
				},
			})
		}

		config.Logger.Info("starting to run protostellar system")
		err = gatewaySys.Serve(ctx, gatewayLis)

		if err != nil {
			config.Logger.Error("failed to serve protostellar system")

			leaveErr := clusterEntry.Leave(ctx)
			if leaveErr != nil {
				config.Logger.Error("failed to leave cluster")
			}

			return err
		}

		err = clusterEntry.Leave(ctx)
		if err != nil {
			config.Logger.Error("failed to leave cluster")
			return err
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
