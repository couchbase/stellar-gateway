package gateway

import (
	"context"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/gateway/system"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/pkg/app_config"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func gatewayStartup(ctx context.Context, config *app_config.Config) error {
	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	// start connecting to the underlying cluster
	config.Logger.Info("linking to couchbase cluster", zap.String("connectionString", config.Config.ConnectionString), zap.String("User", config.Credentials.Username))

	client, err := getReadyCbClient(config.Config.ConnectionString, config.Credentials, config.Logger)
	if err != nil {
		config.Logger.Error("failed to connect to couchbase cluster")
		return err
	}

	config.Logger.Info("connected to couchbase cluster")

	psTopologyManager, topologyManager, err := getTopologyManagers(config)
	if err != nil {
		config.Logger.Error("failed to initialize topology manager")
		return err
	}

	startInstance := func(ctx context.Context, instanceIdx int) error {
		dataImpl := dataimpl.New(&dataimpl.NewOptions{
			Logger:           config.Logger,
			TopologyProvider: psTopologyManager,
			CbClient:         client,
		})

		if config.CredentialsWatcher != nil {
			watchCredentials(config, dataImpl)
		}

		if config.ConfigWatcher != nil {
			watchConfig(config, dataImpl)
		}

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

		clusterEntry, err := topologyManager.Join(ctx, localMemberData)
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

func Run(ctx context.Context, config *app_config.Config) {
	startupCount := 1
	err := gatewayStartup(context.Background(), config)
	//TODO(malscent): daemon mode should start up grpc servers and return errors to client specifying issues connecting to underlying service instead of just restarting whole process
	for err != nil {
		if !config.Config.Daemon {
			config.Logger.Warn("Daemon mode disabled, exiting.", zap.Error(err))
			return
		}
		config.Logger.Warn("Startup of gateway failed.  Retrying...", zap.Int("attempts", startupCount), zap.Duration("interval", time.Second*time.Duration(startupCount)), zap.Error(err))
		time.Sleep(time.Second * time.Duration(startupCount))
		startupCount++
		config.Logger.Debug("Wait time expired. Attempting to restart...")
		err = gatewayStartup(context.Background(), config)
	}
}

func getReadyCbClient(connectionString string, credentials *app_config.CredentialsConfig, logger *zap.Logger) (*gocb.Cluster, error) {
	client, err := gocb.Connect(connectionString, gocb.ClusterOptions{
		Username: credentials.Username,
		Password: credentials.Password,
	})
	if err != nil {
		logger.Error("failed to initialize cb connection")
	}

	err = client.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		logger.Error("failed to wait for couchbase cluster connection")
	}
	return client, err
}

func getTopologyManagers(config *app_config.Config) (*topology.Manager, *clustering.Manager, error) {
	// TODO(brett19): We should use the gocb client to fetch the topologies.
	cbTopologyProvider, err := cbtopology.NewPollingProvider(cbtopology.PollingProviderOptions{
		Fetcher: cbconfig.NewFetcher(cbconfig.FetcherOptions{
			Host:               config.Config.ConnectionString,
			Username:           config.Credentials.Username,
			Password:           config.Credentials.Password,
			Logger:             config.Logger.Named("topology-fetcher"),
			CredentialsWatcher: config.CredentialsWatcher,
			ConfigWatcher:      config.ConfigWatcher,
		}),
		Logger: config.Logger.Named("topology-provider"),
	})
	if err != nil {
		config.Logger.Error("failed to initialize cb topology poller")
		return nil, nil, err
	}

	goclusteringProvider, err := goclustering.NewInProcProvider(goclustering.InProcProviderOptions{})
	if err != nil {
		config.Logger.Error("failed to initialize in-proc clustering provider")
		return nil, nil, err
	}

	clusteringManager := &clustering.Manager{
		Provider: goclusteringProvider,
		Logger:   config.Logger.Named("cluster-manager"),
	}

	psTopologyManager, err := topology.NewManager(&topology.ManagerOptions{
		LocalTopologyProvider:  clusteringManager,
		RemoteTopologyProvider: cbTopologyProvider,
		Logger:                 config.Logger.Named("ps-topology-manager"),
	})

	return psTopologyManager, clusteringManager, err
}

func pickPort(advertisePort int, boundPort int) int {
	if advertisePort != 0 {
		return advertisePort
	}
	return boundPort
}

func watchCredentials(config *app_config.Config, dataImpl *dataimpl.Servers) {
	credsChan := make(chan app_config.CredentialsConfig)
	unsub := config.CredentialsWatcher.Subscribe(credsChan)
	go func() {
		defer unsub()
		for {
			c := <-credsChan
			config.Logger.Info("Updating client due to credentials change", zap.String("old", config.Credentials.Username), zap.String("new", c.Username))
			client, err := getReadyCbClient(config.Config.ConnectionString, &c, config.Logger)
			if err != nil {
				config.Logger.Error("Error getting ready client", zap.Error(err))
				config.Logger.Info("Client update failed")
				continue
			}
			dataImpl.UpdateClient(client)
			config.Credentials = &c
			config.Logger.Info("Client update successful")
		}
	}()
}

func watchConfig(config *app_config.Config, dataImpl *dataimpl.Servers) {
	configChan := make(chan app_config.GeneralConfig)
	unsub := config.ConfigWatcher.Subscribe(configChan)
	go func() {
		defer unsub()
		for {
			c := <-configChan
			if c.ConnectionString != config.Config.ConnectionString {
				config.Logger.Info("Updating client due to connection string change", zap.String("old", config.Config.ConnectionString), zap.String("new", c.ConnectionString))
				client, err := getReadyCbClient(c.ConnectionString, config.Credentials, config.Logger)
				if err != nil {
					config.Logger.Error("Error getting ready client", zap.Error(err))
					config.Logger.Info("Client update failed")
					continue
				}
				dataImpl.UpdateClient(client)
				config.Config = &c
				config.Logger.Info("Client update successful")
			}
		}
	}()
}
