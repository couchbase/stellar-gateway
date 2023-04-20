package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

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
var certPath = flag.String("cert", "", "path to server tls cert")
var keyPath = flag.String("key", "", "path to server private tls key")
var caCertPath = flag.String("cacert", "", "path to root CA cert")

func main() {
	flag.Parse()

	loggerConfig := zap.NewProductionConfig()
	loggerConfig.EncoderConfig.TimeKey = "timestamp"
	loggerConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)

	logger, err := loggerConfig.Build()
	if err != nil {
		log.Printf("failed to initialize logging: %s", err)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("starting %s: %s", version.Application, version.WithBuildNumberAndRevision()))

	// Todo:  Read in log level from CLI or env var
	logLevel := zap.NewAtomicLevel()
	logLevel.SetLevel(zapcore.DebugLevel)

	listenAddress := fmt.Sprintf("0.0.0.0:%v", *webPort)

	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        logger,
		LogLevel:      &logLevel,
		ListenAddress: listenAddress,
	})

	var tlsConfig *gateway.TLSConfig
	if *enableTLS {
		if len(*certPath) == 0 || len(*keyPath) == 0 {
			logger.Error("no server cert or key found to secure serve", zap.String("cert", *certPath), zap.String("key", *keyPath))
			os.Exit(1)
		}
		tlsConfig = &gateway.TLSConfig{
			Cert:   *certPath,
			Key:    *keyPath,
			CaCert: *caCertPath,
		}
	}

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
		TLS:          tlsConfig,
	}

	err = gateway.Run(context.Background(), gatewayConfig)
	if err != nil {
		logger.Error("failed to initialize the gateway", zap.Error(err))
		os.Exit(1)
	}
}
