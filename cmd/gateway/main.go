package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/pkg/version"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var rootCmd = &cobra.Command{
	Version: version.Version,

	Use:   "stellar-gateway",
	Short: "A service for accessing Couchbase over GRPC",

	Run: func(cmd *cobra.Command, args []string) {
		startGateway()
	},
}

var cfgFile string
var watchCfgFile bool
var daemon bool

func init() {
	rootCmd.Flags().StringVar(&cfgFile, "config", "", "specifies a config file to load")
	rootCmd.Flags().BoolVar(&watchCfgFile, "watch-config", false, "indicates whether to watch the config file for changes")
	rootCmd.Flags().BoolVar(&daemon, "daemon", false, "in daemon mode, stellar-gateway will not exit on initial failure")

	configFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	configFlags.String("log-level", "info", "the log level to run at")
	configFlags.String("cb-host", "localhost", "the couchbase server host")
	configFlags.String("cb-user", "Administrator", "the couchbase server username")
	configFlags.String("cb-pass", "password", "the couchbase server password")
	configFlags.String("bind-address", "0.0.0.0", "the local address to bind to")
	configFlags.Int("data-port", 18098, "the data port")
	configFlags.Int("sd-port", 18099, "the sd port")
	configFlags.Int("web-port", 9091, "the web metrics/health port")
	configFlags.Bool("self-sign", false, "specifies to use a self-signed certificate rather than specifying them")
	configFlags.String("cert", "", "path to server tls cert")
	configFlags.String("key", "", "path to server private tls key")
	configFlags.String("cacert", "", "path to root CA cert")
	rootCmd.Flags().AddFlagSet(configFlags)

	_ = viper.BindPFlags(configFlags)
	viper.SetEnvPrefix("stg")
	viper.AutomaticEnv()
}

func startGateway() {
	// initialize the logger
	logLevel := zap.NewAtomicLevel()
	logConfig := zap.NewProductionEncoderConfig()
	logConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(logConfig)
	core := zapcore.NewTee(
		zapcore.NewCore(jsonEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// signal that we are starting
	logger.Info("starting steller-gateway", zap.String("version", version.WithBuildNumberAndRevision()))

	logger.Info("parsed launch configuration",
		zap.String("config", cfgFile),
		zap.Bool("watch-config", watchCfgFile),
		zap.Bool("daemon", daemon))

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		err := viper.ReadInConfig()
		if err != nil {
			logger.Panic("failed to load specified config file", zap.Error(err))
		}
	}

	logLevelStr := viper.GetString("log-level")
	cbHost := viper.GetString("cb-host")
	cbUser := viper.GetString("cb-user")
	cbPass := viper.GetString("cb-pass")
	bindAddress := viper.GetString("bind-address")
	dataPort := viper.GetInt("data-port")
	sdPort := viper.GetInt("sd-port")
	webPort := viper.GetInt("web-port")
	selfSign := viper.GetBool("self-sign")
	certPath := viper.GetString("cert")
	keyPath := viper.GetString("key")
	caCertPath := viper.GetString("cacert")

	logger.Info("parsed gateway configuration",
		zap.String("logLevelStr", logLevelStr),
		zap.String("cbHost", cbHost),
		zap.String("cbUser", cbUser),
		// zap.String("cbPass", cbPass),
		zap.String("bindAddress", bindAddress),
		zap.Int("dataPort", dataPort),
		zap.Int("sdPort", sdPort),
		zap.Int("webPort", webPort),
		zap.Bool("selfSign", selfSign),
		zap.String("certPath", certPath),
		zap.String("keyPath", keyPath),
		zap.String("cacertPath", caCertPath),
	)

	parsedLogLevel, err := zapcore.ParseLevel(logLevelStr)
	if err != nil {
		logger.Warn("invalid log level specified, using INFO instead")
		parsedLogLevel = zapcore.InfoLevel
	}
	logLevel.SetLevel(parsedLogLevel)

	// setup the web service
	webListenAddress := fmt.Sprintf("%s:%v", bindAddress, webPort)
	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        logger,
		LogLevel:      &logLevel,
		ListenAddress: webListenAddress,
	})

	var tlsCertificate tls.Certificate
	if certPath == "" && keyPath == "" {
		// use a self-signed certificate

	} else {
		if certPath == "" || keyPath == "" {
			logger.Error("both cert and key must be specified if either option is specified")
			os.Exit(1)
		}
	}

	if selfSign {
		if certPath != "" || keyPath != "" {
			logger.Error("cannot specify both self-sign along with a cert or key")
			os.Exit(1)
		}

		selfSignedCert, err := selfsignedcert.GenerateCertificate()
		if err != nil {
			logger.Error("failed to generate a self-signed certificate")
			os.Exit(1)
		}

		tlsCertificate = *selfSignedCert
	} else {
		if certPath == "" || keyPath == "" {
			logger.Error("must specify both cert and key unless self-sign is specified")
			os.Exit(1)
		}

		loadedTlsCertificate, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			logger.Error("failed to load tls certificate", zap.Error(err))
			os.Exit(1)
		}

		tlsCertificate = loadedTlsCertificate
	}

	gatewayConfig := &gateway.Config{
		Logger:         logger.Named("gateway"),
		CbConnStr:      cbHost,
		Username:       cbUser,
		Password:       cbPass,
		Daemon:         daemon,
		BindDataPort:   dataPort,
		BindSdPort:     sdPort,
		BindAddress:    bindAddress,
		TlsCertificate: tlsCertificate,
		NumInstances:   1,
	}

	gw, err := gateway.NewGateway(gatewayConfig)
	if err != nil {
		logger.Error("failed to initialize the gateway", zap.Error(err))
		os.Exit(1)
	}

	if watchCfgFile {
		viper.OnConfigChange(func(in fsnotify.Event) {
			err := viper.ReadInConfig()
			if err != nil {
				logger.Warn("a config file change was detected, but the config could not be parsed",
					zap.Error(err))
			}

			newLogLevelStr := viper.GetString("log-level")

			logger.Info("configuration file change detected",
				zap.String("logLevelStr", newLogLevelStr))

			newParsedLogLevel, err := zapcore.ParseLevel(newLogLevelStr)
			if err != nil {
				logger.Warn("invalid log level specified, using INFO instead")
				newParsedLogLevel = zapcore.InfoLevel
			}

			if newParsedLogLevel != parsedLogLevel {
				parsedLogLevel = newParsedLogLevel
				logLevel.SetLevel(parsedLogLevel)

				logger.Info("updated log level",
					zap.String("newLevel", newParsedLogLevel.String()))
			}
		})

		go viper.WatchConfig()
	}

	err = gw.Run(context.Background())
	if err != nil {
		logger.Error("failed to run the gateway", zap.Error(err))
		os.Exit(1)
	}
}

func main() {
	cobra.CheckErr(rootCmd.Execute())
}
