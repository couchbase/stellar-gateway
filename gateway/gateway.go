package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/gateway/system"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/google/uuid"
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

	NumInstances    uint
	StartupCallback func(*StartupInfo)
}

func gatewayStartup(ctx context.Context, config *Config) error {
	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	// start connecting to the underlying cluster
	config.Logger.Info("linking to couchbase cluster", zap.String("connectionString", config.CbConnStr), zap.String("User", config.Username))

	kvHostPort := fmt.Sprintf("%s:11210", config.CbConnStr)
	mgmtHostPort := fmt.Sprintf("%s:8091", config.CbConnStr)

	_, err := cbauth.InternalRetryDefaultInitWithService("stg", mgmtHostPort, config.Username, config.Password)
	if err != nil {
		config.Logger.Error("failed to initialize cbauth connection",
			zap.Error(err),
			zap.String("hostPort", mgmtHostPort),
			zap.String("user", config.Username))
		return err
	}

	agentMgr, err := gocbcorex.CreateAgentManager(ctx, gocbcorex.AgentManagerOptions{
		Logger:    config.Logger.Named("gocbcorex"),
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: []string{mgmtHostPort},
			MemdAddrs: []string{kvHostPort},
		},
	})
	if err != nil {
		config.Logger.Error("failed to connect to couchbase cluster",
			zap.Error(err),
			zap.String("httpAddr", mgmtHostPort),
			zap.String("memdAddr", kvHostPort),
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
		}),
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

	clusteringManager := &clustering.Manager{Provider: goclusteringProvider}

	psTopologyManager, err := topology.NewManager(&topology.ManagerOptions{
		LocalTopologyProvider:  clusteringManager,
		RemoteTopologyProvider: cbTopologyProvider,
	})
	if err != nil {
		config.Logger.Error("failed to initialize topology manager")
		return err
	}

	startInstance := func(ctx context.Context, instanceIdx int) error {
		dataImpl := dataimpl.New(&dataimpl.NewOptions{
			Logger:           config.Logger,
			TopologyProvider: psTopologyManager,
			CbClient:         agentMgr,
		})

		sdImpl := sdimpl.New(&sdimpl.NewOptions{
			Logger:           config.Logger,
			TopologyProvider: psTopologyManager,
		})

		config.Logger.Info("initializing protostellar system")
		gatewaySys, err := system.NewSystem(&system.SystemOptions{
			Logger:   config.Logger,
			DataImpl: dataImpl,
			SdImpl:   sdImpl,
			Metrics:  metrics.GetSnMetrics(),
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

func Run(ctx context.Context, config *Config) error {
	// TODO(malscent): daemon mode should start up grpc servers and return errors
	// to the client specifying issues connecting to underlying service instead of
	// just restarting whole process
	for restarts := 0; ; restarts++ {
		startTime := time.Now()

		err := gatewayStartup(context.Background(), config)
		if err != nil {
			if !config.Daemon {
				return err
			}

			runTime := time.Since(startTime)

			// limit automatic restarts to once every 10 seconds
			waitTime := (10 * time.Second) - runTime
			if waitTime < 0 {
				waitTime = 0
			}

			config.Logger.Warn("startup of gateway failed.  retrying...",
				zap.Error(err),
				zap.Int("attempt", restarts),
				zap.Duration("waitTime", waitTime))
			time.Sleep(waitTime)

			continue
		}

		return nil
	}
}
