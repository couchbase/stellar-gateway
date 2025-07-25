package legacybridge

import (
	"context"
	"os"
	"time"

	"github.com/couchbase/stellar-gateway/client"
	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"github.com/couchbase/stellar-gateway/legacybridge/clustering"
	"github.com/couchbase/stellar-gateway/legacybridge/system"
	"github.com/couchbase/stellar-gateway/legacybridge/topology"
	"github.com/couchbase/stellar-gateway/utils/netutils"
	"github.com/google/uuid"
	"go.uber.org/zap"

	etcd "go.etcd.io/etcd/client/v3"
)

type ServicePorts struct {
	Mgmt      int `json:"m,omitempty"`
	KV        int `json:"k,omitempty"`
	Views     int `json:"v,omitempty"`
	Query     int `json:"q,omitempty"`
	Search    int `json:"s,omitempty"`
	Analytics int `json:"a,omitempty"`
}

type StartupInfo struct {
	MemberID          string
	ServerGroup       string
	AdvertiseAddr     string
	AdvertisePorts    ServicePorts
	AdvertisePortsTLS ServicePorts
}

type Config struct {
	Logger      *zap.Logger
	NodeID      string
	ServerGroup string

	EtcdHost   string
	EtcdPrefix string

	ConnStr  string
	Username string
	Password string

	BindAddress       string
	BindPorts         ServicePorts
	TLSBindPorts      ServicePorts
	AdvertiseAddress  string
	AdvertisePorts    ServicePorts
	AdvertisePortsTLS ServicePorts

	NumInstances    uint
	StartupCallback func(*StartupInfo)
}

func Run(ctx context.Context, config *Config) error {
	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	psClient, err := client.Dial(config.ConnStr, &client.DialOptions{
		Username: config.Username,
		Password: config.Password,
		Logger:   config.Logger.Named("routing-client"),
	})
	if err != nil {
		config.Logger.Error("failed to connect to couchbase", zap.Error(err))
		return err
	}

	var clusteringManager *clustering.Manager
	if config.EtcdHost != "" {
		etcdClient, err := etcd.New(etcd.Config{
			Endpoints:   []string{config.EtcdHost},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			config.Logger.Error("failed to connect to etcd", zap.Error(err))
			return err
		}

		etcdCtx, etcdCtxCancelFn := context.WithDeadline(context.Background(), time.Now().Add(2500*time.Millisecond))
		_, err = etcdClient.Get(etcdCtx, "test-key")
		etcdCtxCancelFn()
		if err != nil {
			config.Logger.Error("failed to validate etcd connection", zap.Error(err))
			return err
		}

		goclusteringProvider, err := goclustering.NewEtcdProvider(goclustering.EtcdProviderOptions{
			EtcdClient: etcdClient,
			KeyPrefix:  config.EtcdPrefix + "/bridge/topology",
			Logger:     config.Logger.Named("clustering-provider"),
		})
		if err != nil {
			config.Logger.Error("failed to initialize etcd clustering provider", zap.Error(err))
			return err
		}

		clusteringManager = &clustering.Manager{Provider: goclusteringProvider, Logger: config.Logger.Named("clustering-manager")}
	} else {
		goclusteringProvider, err := goclustering.NewInProcProvider(goclustering.InProcProviderOptions{})
		if err != nil {
			config.Logger.Error("failed to initialize in-proc clustering provider", zap.Error(err))
			return err
		}

		clusteringManager = &clustering.Manager{Provider: goclusteringProvider, Logger: config.Logger.Named("clustering-manager")}
	}

	legacyTopologyManager, err := topology.NewManager(&topology.ManagerOptions{
		LocalTopologyProvider:  clusteringManager,
		RemoteTopologyProvider: psClient,
		Logger:                 config.Logger.Named("legacyTopologyManager"),
	})
	if err != nil {
		config.Logger.Error("failed to initialize topology manager", zap.Error(err))
		return err
	}

	startInstance := func(ctx context.Context, instanceIdx int) error {
		config.Logger.Info("initializing legacy system")
		legacySys, err := system.NewSystem(&system.SystemOptions{
			Logger:           config.Logger,
			TopologyProvider: legacyTopologyManager,
			Client:           psClient,
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy", zap.Error(err))
		}

		ports := system.ServicePorts{
			Mgmt:  config.BindPorts.Mgmt,
			KV:    config.BindPorts.KV,
			Query: config.BindPorts.Query,
		}
		tlsPorts := system.ServicePorts{
			Mgmt:  config.TLSBindPorts.Mgmt,
			KV:    config.TLSBindPorts.KV,
			Query: config.TLSBindPorts.Query,
		}

		// the non-0 instance uses randomized ports
		if instanceIdx > 0 {
			ports = system.ServicePorts{}
			tlsPorts = system.ServicePorts{}
		}

		legacyLis, err := system.NewListeners(&system.ListenersOptions{
			Address:  config.BindAddress,
			Ports:    ports,
			TLSPorts: tlsPorts,
		})
		if err != nil {
			config.Logger.Error("error creating legacy proxy listeners", zap.Error(err))
			return err
		}

		advertiseAddr := config.AdvertiseAddress
		if advertiseAddr == "" {
			advertiseAddr, err = netutils.GetAdvertiseAddress(config.BindAddress)
			if err != nil {
				config.Logger.Error("failed to identify advertise address", zap.Error(err))
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
			Mgmt:  pickPort(config.AdvertisePorts.Mgmt, legacyLis.BoundMgmtPort()),
			KV:    pickPort(config.AdvertisePorts.KV, legacyLis.BoundQueryPort()),
			Query: pickPort(config.AdvertisePorts.Query, legacyLis.BoundQueryPort()),

			MgmtTls:  pickPort(config.AdvertisePortsTLS.Mgmt, legacyLis.BoundMgmtTLSPort()),
			KVTls:    pickPort(config.AdvertisePortsTLS.KV, legacyLis.BoundKVTLSPort()),
			QueryTls: pickPort(config.AdvertisePortsTLS.Query, legacyLis.BoundQueryTLSPort()),
		}

		localMemberData := &clustering.Member{
			MemberID:       nodeID,
			ServerGroup:    serverGroup,
			AdvertiseAddr:  advertiseAddr,
			AdvertisePorts: advertisePorts,
		}

		clusterEntry, err := clusteringManager.Join(ctx, localMemberData)
		if err != nil {
			config.Logger.Error("failed to join cluster", zap.Error(err))
			os.Exit(1)
		}

		if instanceIdx == 0 && config.StartupCallback != nil {
			config.StartupCallback(&StartupInfo{
				MemberID:      nodeID,
				ServerGroup:   serverGroup,
				AdvertiseAddr: advertiseAddr,
				AdvertisePorts: ServicePorts{
					Mgmt:  advertisePorts.Mgmt,
					KV:    advertisePorts.KV,
					Query: advertisePorts.Query,
				},
				AdvertisePortsTLS: ServicePorts{
					Mgmt:  advertisePorts.MgmtTls,
					KV:    advertisePorts.KVTls,
					Query: advertisePorts.QueryTls,
				},
			})
		}
		config.Logger.Info("starting to run legacy system")
		err = legacySys.Serve(ctx, legacyLis)

		if err != nil {
			config.Logger.Error("failed to serve legacy system", zap.Error(err))

			leaveErr := clusterEntry.Leave(ctx)
			if leaveErr != nil {
				config.Logger.Error("failed to leave cluster", zap.Error(err))
			}

			return err
		}

		err = clusterEntry.Leave(ctx)
		if err != nil {
			config.Logger.Error("failed to leave cluster", zap.Error(err))
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
