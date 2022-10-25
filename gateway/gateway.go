package gateway

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
	"github.com/couchbase/stellar-nebula/contrib/cbtopology"
	"github.com/couchbase/stellar-nebula/contrib/goclustering"
	"github.com/couchbase/stellar-nebula/gateway/clustering"
	"github.com/couchbase/stellar-nebula/gateway/dataimpl"
	"github.com/couchbase/stellar-nebula/gateway/sdimpl"
	"github.com/couchbase/stellar-nebula/gateway/system"
	"github.com/couchbase/stellar-nebula/gateway/topology"
	"github.com/couchbase/stellar-nebula/utils/netutils"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Config struct {
	Logger      *zap.Logger
	NodeID      string
	ServerGroup string

	CbConnStr string
	Username  string
	Password  string

	BindAddress       string
	BindDataPort      int
	BindSdPort        int
	AdvertiseAddress  string
	AdvertiseDataPort int
	AdvertiseSdPort   int

	NumInstances       uint
	MembershipCallback func(clustering.Member)
}

func Run(ctx context.Context, config *Config) error {
	// NodeID must not be blank, so lets generate a unique UUID if one wasn't provided...
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	serverGroup := config.ServerGroup

	// start connecting to the underlying cluster
	log.Printf("linking to couchbase cluster at: %s (user: %s)", config.CbConnStr, config.Username)

	client, err := gocb.Connect(config.CbConnStr, gocb.ClusterOptions{
		Username: config.Username,
		Password: config.Password,
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

	// TODO(brett19): We should use the gocb client to fetch the topologies.
	cbTopologyProvider, err := cbtopology.NewPollingProvider(cbtopology.PollingProviderOptions{
		Fetcher: cbconfig.NewFetcher(cbconfig.FetcherOptions{
			Host:     config.CbConnStr,
			Username: config.Username,
			Password: config.Password,
		}),
	})
	if err != nil {
		log.Fatalf("failed to initialize cb topology poller: %s", err)
	}

	goclusteringProvider, err := goclustering.NewInProcProvider(goclustering.InProcProviderOptions{})
	if err != nil {
		log.Printf("failed to initialize in-proc clustering provider: %s", err)
		return err
	}

	clusteringManager := &clustering.Manager{Provider: goclusteringProvider}

	psTopologyManager, err := topology.NewManager(&topology.ManagerOptions{
		LocalTopologyProvider:  clusteringManager,
		RemoteTopologyProvider: cbTopologyProvider,
	})
	if err != nil {
		log.Printf("failed to initialize topology manager: %s", err)
		return err
	}

	startInstance := func(ctx context.Context, instanceIdx int) error {
		dataImpl := dataimpl.New(&dataimpl.NewOptions{
			Logger:           config.Logger,
			TopologyProvider: psTopologyManager,
			CbClient:         client,
		})

		sdImpl := sdimpl.New(&sdimpl.NewOptions{
			Logger:           config.Logger,
			TopologyProvider: psTopologyManager,
		})

		log.Printf("initializing protostellar system")
		legacySys, err := system.NewSystem(&system.SystemOptions{
			Logger:   config.Logger,
			DataImpl: dataImpl,
			SdImpl:   sdImpl,
		})
		if err != nil {
			log.Printf("error creating legacy proxy: %s", err)
		}

		dataPort := config.BindDataPort
		sdPort := config.BindSdPort

		// the non-0 instance uses randomized ports
		if instanceIdx > 0 {
			dataPort = 0
			sdPort = 0
		}

		legacyLis, err := system.NewListeners(&system.ListenersOptions{
			Address:  config.BindAddress,
			DataPort: dataPort,
			SdPort:   sdPort,
		})
		if err != nil {
			log.Printf("error creating legacy proxy listeners: %s", err)
			return err
		}

		advertiseAddr := config.AdvertiseAddress
		if advertiseAddr == "" {
			advertiseAddr, err = netutils.GetAdvertiseAddress(config.BindAddress)
			if err != nil {
				log.Printf("failed to identify advertise address: %s", err)
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
			PS: pickPort(config.AdvertiseDataPort, legacyLis.BoundDataPort()),
			SD: pickPort(config.AdvertiseSdPort, legacyLis.BoundSdPort()),
		}

		localMemberData := &clustering.Member{
			MemberID:       nodeID,
			ServerGroup:    serverGroup,
			AdvertiseAddr:  advertiseAddr,
			AdvertisePorts: advertisePorts,
		}

		if instanceIdx == 0 && config.MembershipCallback != nil {
			config.MembershipCallback(*localMemberData)
		}

		clusterEntry, err := clusteringManager.Join(ctx, localMemberData)
		if err != nil {
			log.Fatalf("failed to join cluster: %s", err)
			os.Exit(1)
		}

		log.Printf("starting to run protostellar system")
		err = legacySys.Serve(ctx, legacyLis)

		if err != nil {
			log.Printf("failed to serve protostellar system: %s", err)

			leaveErr := clusterEntry.Leave(ctx)
			if leaveErr != nil {
				log.Printf("failed to leave cluster: %s", err)
			}

			return err
		}

		err = clusterEntry.Leave(ctx)
		if err != nil {
			log.Printf("failed to leave cluster: %s", err)
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
