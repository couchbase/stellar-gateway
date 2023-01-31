package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/couchbase/stellar-nebula/gateway"
	"github.com/couchbase/stellar-nebula/pkg/version"
	"go.uber.org/zap"
)

var cbHost = flag.String("cb-host", "127.0.0.1", "the couchbase server host")
var cbUser = flag.String("cb-user", "Administrator", "the couchbase server username")
var cbPass = flag.String("cb-pass", "password", "the couchbase server password")
var cbConnTimeout = flag.Int("cb-timeout", 10, "the couchbase server connection timeout")

func main() {
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Printf("failed to initialize logging: %s", err)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Starting %s: %s", version.Application, version.WithBuildNumberAndRevision()))

	// In order to start the bridge, we need to know where the gateway is running,
	// so we use a channel and a hook in the gateway to get this.
	gatewayConnStrCh := make(chan string, 100)


	dataPort := 18098
	sdPort := 18099

	if *cbConnTimeout < 10 {
		*cbConnTimeout = 10
	}

	err = gateway.Run(context.Background(), &gateway.Config{
		Logger:       logger.Named("gateway"),
		CbConnStr:    *cbHost,
		Username:     *cbUser,
		Password:     *cbPass,
		ConnTimeout: *cbConnTimeout,
		BindDataPort: dataPort,
		BindSdPort:   sdPort,
		NumInstances: 1,

		StartupCallback: func(m *gateway.StartupInfo) {
			gatewayConnStrCh <- fmt.Sprintf("%s:%d", m.AdvertiseAddr, m.AdvertisePorts.PS)
		},
	})
	if err != nil {
		logger.Error("failed to initialize the gateway: %s", zap.Error(err))
		os.Exit(1)
	}

	gatewayAddr := <-gatewayConnStrCh
	logger.Info("debug setup got a gateway address", zap.String("addr", gatewayAddr))
}
