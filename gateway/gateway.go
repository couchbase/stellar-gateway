package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
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
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var ErrCbAuthAlreadyInitialised = errors.New("cbauth is already initialized")

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

type TLSConfig struct {
	Cert, Key, CaCert string
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

	TLS *TLSConfig
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

func PollServer(hostname string, logger *zap.Logger) error {

	// check if it starts with http
	if !strings.HasPrefix(hostname, "http") {
		hostname = fmt.Sprintf("http://%s", hostname)
	}

	logger.Info(fmt.Sprintf("polling couchbase server: %s", hostname))

	res, err := http.Get(hostname)
	if err != nil {
		logger.Warn(fmt.Sprintf("couchbase server unreachable: %s", hostname), zap.Error(err))
		return err
	}

	if res.StatusCode != 200 {
		err := fmt.Errorf("expected 200 found %d", res.StatusCode)
		logger.Warn(fmt.Sprintf("couchbase server unreachable: %s", hostname), zap.Error(err))
		return err
	}

	return nil
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

	mgmtHostPort, err := connStrToMgmtHostPort(config.CbConnStr)
	if err != nil {
		config.Logger.Error("failed to parse connection string", zap.Error(err))
		return err
	}

	if err := PollServer(mgmtHostPort, config.Logger); err != nil {
		return err
	}

	_, err = cbauth.InternalRetryDefaultInitWithService("stg", mgmtHostPort, config.Username, config.Password)
	if err != nil {
		// return error if any errors other than ErrCbAuthAlreadyInitialised
		if !errors.Is(err, ErrCbAuthAlreadyInitialised) {
			config.Logger.Error("failed to initialize cbauth connection",
				zap.Error(err),
				zap.String("hostPort", mgmtHostPort),
				zap.String("user", config.Username))
			return err
		}
	}

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

	var serverTlsCredOpts []grpc.ServerOption
	if config.TLS != nil {
		getServerTlsCreds := func(cert, key, caCert string) (credentials.TransportCredentials, error) {
			serverCert, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, err
			}

			var caCertPool *x509.CertPool
			if len(caCert) > 0 {
				caCert, err := ioutil.ReadFile(caCert)
				if err != nil {
					return nil, err
				}

				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					return nil, err
				}
			}

			config := &tls.Config{
				Certificates: []tls.Certificate{serverCert},
				RootCAs:      caCertPool,
				ClientAuth:   tls.NoClientCert, // TODO(abose): mTLS NOT implemeneted at this point.
			}
			return credentials.NewTLS(config), nil
		}

		tlsCreds, err := getServerTlsCreds(config.TLS.Cert, config.TLS.Key, config.TLS.CaCert)
		if err != nil {
			config.Logger.Error("error loading TLS credentials")
			return err
		}

		serverTlsCredOpts = append(serverTlsCredOpts, grpc.Creds(tlsCreds))
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
			Logger:      config.Logger.Named("gateway-system"),
			DataImpl:    dataImpl,
			SdImpl:      sdImpl,
			Metrics:     metrics.GetSnMetrics(),
			TlsCredOpts: serverTlsCredOpts,
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

		err := gatewayStartup(ctx, config)
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
