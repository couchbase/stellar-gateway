package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/couchbase/stellar-gateway/gateway"
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
var daemon = flag.Bool("daemon", false, "When in daemon mode, stellar-gateway will restart on failure")

var enableTLS = flag.Bool("secure", false, "enable SSL/TLS")
var	certPath = flag.String("cert", "", "path to server tls cert")
var	keyPath = flag.String("key", "", "path to server private tls key")

func main() {
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Printf("failed to initialize logging: %s", err)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("starting %s: %s", version.Application, version.WithBuildNumberAndRevision()))

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

	gatewayConfig := &gateway.Config{
		Logger:       logger.Named("gateway"),
		CbConnStr:    *cbHost,
		Username:     *cbUser,
		Password:     *cbPass,
		Daemon:       *daemon,
		BindDataPort: *dataPort,
		BindSdPort:   *sdPort,
		BindAddress:  "0.0.0.0",
		NumInstances: 1,

		StartupCallback: func(m *gateway.StartupInfo) {
			gatewayConnStrCh <- fmt.Sprintf("%s:%d", m.AdvertiseAddr, m.AdvertisePorts.PS)
		},

		EnableTLS: *enableTLS,
		Cert: *certPath,
		Key: *keyPath,
	}

	err = gateway.Run(context.Background(), gatewayConfig)
	if err != nil {
		logger.Error("failed to initialize the gateway", zap.Error(err))
		os.Exit(1)
	}
}
