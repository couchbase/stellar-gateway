package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/pkg/version"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var cbHost = flag.String("cb-host", "127.0.0.1", "the couchbase server host")
var cbUser = flag.String("cb-user", "Administrator", "the couchbase server username")
var cbPass = flag.String("cb-pass", "password", "the couchbase server password")
var dataPort = flag.Int("data-port", 18098, "the data port")
var sdPort = flag.Int("sd-port", 18099, "the sd port")
var webPort = flag.Int("web-port", 9091, "the web metrics/health port")

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

	// Todo:  Read in log level from CLI or env var
	logLevel := zap.NewAtomicLevel()
	logLevel.SetLevel(zapcore.DebugLevel)

	listenAddress := fmt.Sprintf("0.0.0.0:%v", *webPort)

	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        logger,
		LogLevel:      &logLevel,
		ListenAddress: listenAddress,
	})

	err = gateway.Run(context.Background(), &gateway.Config{
		Logger:       logger.Named("gateway"),
		CbConnStr:    *cbHost,
		Username:     *cbUser,
		Password:     *cbPass,
		BindDataPort: *dataPort,
		BindSdPort:   *sdPort,
		BindAddress:  "0.0.0.0",
		NumInstances: 1,
		SnMetrics:    metrics.GetSnMetrics(),

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
