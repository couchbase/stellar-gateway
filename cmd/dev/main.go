package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/legacybridge"
	"go.uber.org/zap"
)

var numInstances = flag.Uint("num-instances", 3, "how many instances to run")
var noDefaultPorts = flag.Bool("no-default-ports", false, "whether to avoid using default ports")
var noLegacyDev = flag.Bool("no-legacy", false, "whether to disable the legacy bridge")
var cbHost = flag.String("cb-host", "127.0.0.1", "the couchbase server host")
var cbUser = flag.String("cb-user", "Administrator", "the couchbase server username")
var cbPass = flag.String("cb-pass", "password", "the couchbase server password")

func main() {
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Printf("failed to initialize logging: %s", err)
		os.Exit(1)
	}

	buildVersion := buildversion.GetVersion("github.com/couchbase/stellar-gateway")
	logger.Info("starting stellar-gateway", zap.String("version", buildVersion))

	// In order to start the bridge, we need to know where the gateway is running,
	// so we use a channel and a hook in the gateway to get this.
	gatewayConnStrCh := make(chan string, 100)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		dataPort := 0
		sdPort := 0
		if !*noDefaultPorts {
			dataPort = 18098
			sdPort = 18099
		}

		gw, err := gateway.NewGateway(&gateway.Config{
			Logger:       logger.Named("gateway"),
			CbConnStr:    *cbHost,
			Username:     *cbUser,
			Password:     *cbPass,
			BindDataPort: dataPort,
			BindSdPort:   sdPort,
			NumInstances: *numInstances,

			StartupCallback: func(m *gateway.StartupInfo) {
				gatewayConnStrCh <- fmt.Sprintf("%s:%d", m.AdvertiseAddr, m.AdvertisePorts.PS)
			},
		})
		if err != nil {
			log.Printf("failed to initialize the gateway: %s", err)
			os.Exit(1)
		}

		err = gw.Run(context.Background())
		if err != nil {
			log.Printf("failed to run the gateway: %s", err)
			os.Exit(1)
		}

		wg.Done()
	}()

	gatewayAddr := <-gatewayConnStrCh
	logger.Info("debug setup got a gateway address", zap.String("addr", gatewayAddr))

	if !*noLegacyDev {
		wg.Add(1)
		go func() {
			bindPorts := legacybridge.ServicePorts{}
			tlsBindPorts := legacybridge.ServicePorts{}

			if !*noDefaultPorts {
				bindPorts = legacybridge.ServicePorts{
					Mgmt:      8091,
					KV:        11210,
					Views:     8092,
					Query:     8093,
					Search:    8094,
					Analytics: 8095,
				}

				// TODO(brett19): Add a default TLS config and uncomment below.
				/*
					tlsBindPorts = legacybridge.ServicePorts{
						Mgmt:      18091,
						KV:        11207,
						Views:     18092,
						Query:     18093,
						Search:    18094,
						Analytics: 18095,
					}
				*/
			}

			err := legacybridge.Run(context.Background(), &legacybridge.Config{
				Logger:       logger.Named("bridge"),
				ConnStr:      gatewayAddr,
				BindPorts:    bindPorts,
				TLSBindPorts: tlsBindPorts,
				NumInstances: *numInstances,
			})
			if err != nil {
				logger.Error("failed to initialize the legacy bridge", zap.Error(err))
				os.Exit(1)
			}

			wg.Done()
		}()
	}

	wg.Wait()
}
