package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/app_config"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/gateway/system"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

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


func gatewayStartup(ctx context.Context, config *app_config.Config) error {
	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	// start connecting to the underlying cluster
	config.Logger.Info("linking to couchbase cluster", zap.String("connectionString", config.Config.ConnectionString), zap.String("User", config.Credentials.Password))

	mgmtHostPort, err := connStrToMgmtHostPort(config.Config.ConnectionString)
	if err != nil {
		config.Logger.Error("failed to parse connection string", zap.Error(err))
		return err
	}

	_, err = cbauth.InternalRetryDefaultInitWithService("stg", mgmtHostPort, config.Credentials.Username, config.Credentials.Password)
	if err != nil {
		config.Logger.Error("failed to initialize cbauth connection",
			zap.Error(err),
			zap.String("hostPort", mgmtHostPort),
			zap.String("user", config.Credentials.Username))
		return err
	}

	agentMgr, err := gocbcorex.CreateAgentManager(ctx, gocbcorex.AgentManagerOptions{
		Logger:    config.Logger.Named("gocbcorex"),
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: config.Credentials.Username,
			Password: config.Credentials.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: []string{mgmtHostPort},
		},
	})
	if err != nil {
		config.Logger.Error("failed to connect to couchbase cluster",
			zap.Error(err),
			zap.String("httpAddr", mgmtHostPort),
			zap.String("user", config.Credentials.Username))
		return err
	}

	config.Logger.Info("connected agent manager",
		zap.Any("agent", agentMgr))

	config.Logger.Info("connected to couchbase cluster")

	// TODO(brett19): We should use the gocb client to fetch the topologies.
	cbTopologyProvider, err := cbtopology.NewPollingProvider(cbtopology.PollingProviderOptions{
		Fetcher: cbconfig.NewFetcher(cbconfig.FetcherOptions{
			Host:     config.Config.ConnectionString,
			Username: config.Credentials.Username,
			Password: config.Credentials.Password,
			Logger: config.Logger.Named("fetcher"),
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
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy")
			return err
		}

		dataPort := config.Config.DataPort
		sdPort := config.Config.SdPort

		// the non-0 instance uses randomized ports
		if instanceIdx > 0 {
			dataPort = 0
			sdPort = 0
		}

		gatewayLis, err := system.NewListeners(&system.ListenersOptions{
			Address:  config.Config.BindAddress,
			DataPort: dataPort,
			SdPort:   sdPort,
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy listeners")
			return err
		}

		advertiseAddr := config.AdvertiseAddress
		if advertiseAddr == "" {
			advertiseAddr, err = netutils.GetAdvertiseAddress(config.Config.BindAddress)
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
			config.StartupCallback(&app_config.StartupInfo{
				MemberID:      nodeID,
				ServerGroup:   serverGroup,
				AdvertiseAddr: advertiseAddr,
				AdvertisePorts: app_config.ServicePorts{
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

func Run(ctx context.Context, config *app_config.Config) error {
	// TODO(malscent): daemon mode should start up grpc servers and return errors
	// to the client specifying issues connecting to underlying service instead of
	// just restarting whole process
	for restarts := 0; ; restarts++ {
		startTime := time.Now()

		err := gatewayStartup(ctx, config)
		if err != nil {
			if !config.Config.Daemon {
				return err
			}

			runTime := time.Since(startTime)

			// limit automatic restarts to once every 10 seconds
			waitTime := (10 * time.Second) - runTime
			config.Logger.Debug("Wait time", zap.Duration("waitSeconds", waitTime))
			if waitTime < 0 {
				waitTime = 0
			}

			config.Logger.Warn("startup of gateway failed.  retrying...",
				zap.Error(err),
				zap.Int("attempt", restarts),
				zap.Duration("waitTime", waitTime))
			time.Sleep(waitTime)
			config.Logger.Debug("Wait time completed", zap.Duration("waitSeconds", waitTime))
			continue
		}

		return nil
	}
}
