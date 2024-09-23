package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
	"github.com/couchbase/stellar-gateway/utils/secretsmanager"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var buildVersion string = buildversion.GetVersion("github.com/couchbase/stellar-gateway")

var rootCmd = &cobra.Command{
	Version: buildVersion,

	Use:   "stellar-gateway",
	Short: "A service for accessing Couchbase over GRPC",

	Run: func(cmd *cobra.Command, args []string) {
		if autoRestart && !autoRestartProc {
			startGatewayWatchdog()
			return
		}

		startGateway()
	},
}

var cfgFile string
var watchCfgFile bool
var daemon bool
var autoRestart bool
var autoRestartProc bool

func init() {
	rootCmd.Flags().StringVar(&cfgFile, "config", "", "specifies a config file to load")
	rootCmd.Flags().BoolVar(&watchCfgFile, "watch-config", false, "indicates whether to watch the config file for changes")
	rootCmd.Flags().BoolVar(&daemon, "daemon", false, "in daemon mode, stellar-gateway will not exit on initial failure")
	rootCmd.Flags().BoolVar(&autoRestart, "auto-restart", false, "in auto-restart mode, we run in a child process to auto-restart on failure")
	rootCmd.Flags().BoolVar(&autoRestartProc, "auto-restart-proc", false, "in auto-restart mode, indicates we are the child process")
	_ = rootCmd.Flags().MarkHidden("auto-restart-proc")

	configFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	configFlags.String("log-level", "info", "the log level to run at")
	configFlags.String("cb-host", "localhost", "the couchbase server host")
	configFlags.String("cb-user", "Administrator", "the couchbase server username")
	configFlags.String("cb-pass", "password", "the couchbase server password")
	configFlags.String("bind-address", "0.0.0.0", "the local address to bind to")
	configFlags.Int("data-port", 18098, "the data port")
	configFlags.Int("sd-port", 18099, "the sd port")
	configFlags.Int("dapi-port", -1, "the data api port")
	configFlags.Int("web-port", 9091, "the web metrics/health port")
	configFlags.Bool("self-sign", false, "specifies to allow a self-signed certificate")
	configFlags.String("cert", "", "path to default tls cert")
	configFlags.String("key", "", "path to default private tls key")
	configFlags.String("grpc-cert", "", "path to grpc tls cert for GRPC")
	configFlags.String("grpc-key", "", "path to grpc private tls key for GRPC")
	configFlags.String("dapi-cert", "", "path to data api tls cert for Data API")
	configFlags.String("dapi-key", "", "path to data api private tls key for Data API")
	configFlags.Int("rate-limit", 0, "specifies the maximum requests per second to allow")
	configFlags.String("otlp-endpoint", "", "opentelemetry endpoint to send telemetry to")
	configFlags.Bool("disable-otlp-traces", false, "disable sending traces to otlp")
	configFlags.Bool("disable-otlp-metrics", false, "disable sending metrics to otlp")
	configFlags.Bool("trace-everything", false, "enables tracing of all components")
	configFlags.String("dapi-proxy-services", "", "specifies services exposed via _p endpoint proxies")
	configFlags.Bool("debug", false, "enable debug mode")
	configFlags.String("cpuprofile", "", "write cpu profile to a file")
	configFlags.String("cb-creds-aws-id", "", "id of secret in aws sm storing couchbase server credentials")
	configFlags.String("cb-creds-aws-region", "", "region of cb-creds-aws-id secret")
	configFlags.String("cb-creds-azure-id", "", "id of secret in azure kv storing couchbase server credentials")
	configFlags.String("cb-creds-azure-vault-name", "", "name of key vault storing cb-creds-azure-id")
	configFlags.String("cb-creds-gcp-id", "", "id of secret in gcp sm storing couchbase server password")
	configFlags.String("cb-creds-gcp-project-id", "", "id of project containing cb-creds-gcp-id")
	rootCmd.Flags().AddFlagSet(configFlags)

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.SetEnvPrefix("stg")
	viper.AutomaticEnv()

	_ = viper.BindPFlags(configFlags)
}

func initTelemetry(
	ctx context.Context,
	logger *zap.Logger,
	otlpEndpoint string,
	enableTraces bool,
	enableMetrics bool,
	traceEverything bool,
) (
	*sdktrace.TracerProvider,
	*sdkmetric.MeterProvider,
	error,
) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String("couchbase-cloud-native-gateway"),
		),
	)
	if err != nil {
		if res == nil {
			return nil, nil, err
		}

		logger.Warn("failed to setup some part of opentelemetry resource", zap.Error(err))
	}

	promExp, err := prometheus.New()
	if err != nil {
		return nil, nil, err
	}

	var meterProvider *sdkmetric.MeterProvider
	if !enableMetrics || otlpEndpoint == "" {
		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(promExp),
		)
	} else {
		metricExp, err := otlpmetricgrpc.New(
			ctx,
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint))
		if err != nil {
			return nil, nil, err
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(promExp),
			sdkmetric.WithReader(
				sdkmetric.NewPeriodicReader(
					metricExp,
				),
			),
			sdkmetric.WithReader(promExp),
		)
	}

	var tracerProvider *sdktrace.TracerProvider
	if !enableTraces || otlpEndpoint == "" {
		// we can just return nil here...
	} else {
		traceClient := otlptracegrpc.NewClient(
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(otlpEndpoint))
		traceExp, err := otlptrace.New(ctx, traceClient)
		if err != nil {
			return nil, nil, err
		}

		baseTracing := sdktrace.NeverSample()
		if traceEverything {
			baseTracing = sdktrace.AlwaysSample()
		}

		bsp := sdktrace.NewBatchSpanProcessor(traceExp)
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.ParentBased(baseTracing)),
			sdktrace.WithResource(res),
			sdktrace.WithSpanProcessor(bsp),
		)
	}

	return tracerProvider, meterProvider, nil
}

func getLogger() (zap.AtomicLevel, *zap.Logger) {
	logLevel := zap.NewAtomicLevel()
	logConfig := zap.NewProductionEncoderConfig()
	logConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(logConfig)
	core := zapcore.NewTee(
		zapcore.NewCore(jsonEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logLevel, logger
}

type config struct {
	logLevelStr           string
	cbHost                string
	cbUser                string
	cbPass                string
	bindAddress           string
	dataPort              int
	sdPort                int
	webPort               int
	dapiPort              int
	selfSign              bool
	certPath              string
	keyPath               string
	grpcCertPath          string
	grpcKeyPath           string
	dapiCertPath          string
	dapiKeyPath           string
	rateLimit             int
	otlpEndpoint          string
	disableOtlpTraces     bool
	disableOtlpMetrics    bool
	traceEverything       bool
	dapiProxyServices     string
	debug                 bool
	cpuprofile            string
	cbCredsAwsId          string
	cbCredsAwsRegion      string
	cbCredsAzureId        string
	cbCredsAzureVaultName string
	cbCredsGcpId          string
	cbCredsGcpProjectId   string
}

func readConfig(logger *zap.Logger) *config {
	config := &config{
		logLevelStr:           viper.GetString("log-level"),
		cbHost:                viper.GetString("cb-host"),
		cbUser:                viper.GetString("cb-user"),
		cbPass:                viper.GetString("cb-pass"),
		bindAddress:           viper.GetString("bind-address"),
		dataPort:              viper.GetInt("data-port"),
		sdPort:                viper.GetInt("sd-port"),
		webPort:               viper.GetInt("web-port"),
		dapiPort:              viper.GetInt("dapi-port"),
		selfSign:              viper.GetBool("self-sign"),
		certPath:              viper.GetString("cert"),
		keyPath:               viper.GetString("key"),
		grpcCertPath:          viper.GetString("grpc-cert"),
		grpcKeyPath:           viper.GetString("grpc-key"),
		dapiCertPath:          viper.GetString("dapi-cert"),
		dapiKeyPath:           viper.GetString("dapi-key"),
		rateLimit:             viper.GetInt("rate-limit"),
		otlpEndpoint:          viper.GetString("otlp-endpoint"),
		disableOtlpTraces:     viper.GetBool("disable-otlp-traces"),
		disableOtlpMetrics:    viper.GetBool("disable-otlp-metrics"),
		traceEverything:       viper.GetBool("trace-everything"),
		dapiProxyServices:     viper.GetString("dapi-proxy-services"),
		debug:                 viper.GetBool("debug"),
		cpuprofile:            viper.GetString("cpuprofile"),
		cbCredsAwsId:          viper.GetString("cb-creds-aws-id"),
		cbCredsAwsRegion:      viper.GetString("cb-creds-aws-region"),
		cbCredsAzureId:        viper.GetString("cb-creds-azure-id"),
		cbCredsAzureVaultName: viper.GetString("cb-creds-azure-vault-name"),
		cbCredsGcpId:          viper.GetString("cb-creds-gcp-id"),
		cbCredsGcpProjectId:   viper.GetString("cb-creds-gcp-project-id"),
	}

	logger.Info("parsed gateway configuration",
		zap.String("logLevelStr", config.logLevelStr),
		zap.String("cbHost", config.cbHost),
		zap.String("cbUser", config.cbUser),
		// zap.String("cbPass", config.cbPass),
		zap.String("bindAddress", config.bindAddress),
		zap.Int("dataPort", config.dataPort),
		zap.Int("sdPort", config.sdPort),
		zap.Int("webPort", config.webPort),
		zap.Int("dapiPort", config.dapiPort),
		zap.Bool("selfSign", config.selfSign),
		zap.String("certPath", config.certPath),
		zap.String("keyPath", config.keyPath),
		zap.String("grpcCertPath", config.grpcCertPath),
		zap.String("grpcKeyPath", config.grpcKeyPath),
		zap.String("dapiCertPath", config.dapiCertPath),
		zap.String("dapiKeyPath", config.dapiKeyPath),
		zap.Int("rateLimit", config.rateLimit),
		zap.String("otlpEndpoint", config.otlpEndpoint),
		zap.Bool("disableOtlpTraces", config.disableOtlpTraces),
		zap.Bool("disableOtlpMetrics", config.disableOtlpMetrics),
		zap.Bool("traceEverything", config.traceEverything),
		zap.String("dapiProxyServices", config.dapiProxyServices),
		zap.Bool("debug", config.debug),
		zap.String("cpuprofile", config.cpuprofile),
		zap.String("cbCredsAwsId", config.cbCredsAwsId),
		zap.String("cbCredsAwsRegion", config.cbCredsAwsRegion),
		zap.String("cbCredsAzureId", config.cbCredsAzureId),
		zap.String("cbCredsAzureVaultName", config.cbCredsAzureVaultName),
		zap.String("cbCredsGcpId", config.cbCredsGcpId),
		zap.String("cbCredsGcpId", config.cbCredsGcpProjectId))

	return config
}

func startGateway() {
	// initialize the logger
	logLevel, logger := getLogger()

	// signal that we are starting
	buildVersion := buildversion.GetVersion("github.com/couchbase/stellar-gateway")
	logger.Info("starting stellar-gateway", zap.String("version", buildVersion))

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

	config := readConfig(logger)

	parsedLogLevel, err := zapcore.ParseLevel(config.logLevelStr)
	if err != nil {
		logger.Warn("invalid log level specified, using INFO instead")
		parsedLogLevel = zapcore.InfoLevel
	}
	logLevel.SetLevel(parsedLogLevel)

	// setup profiling
	if config.cpuprofile != "" {
		f, err := os.Create(config.cpuprofile)
		if err != nil {
			logger.Error("failed to create cpu profile file", zap.Error(err))
			os.Exit(1)
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			logger.Error("failed to start cpu profiling", zap.Error(err))
			os.Exit(1)
		}

		defer pprof.StopCPUProfile()
	}

	// setup tracing
	otlpTracerProvider, otlpMeterProvider, err :=
		initTelemetry(context.Background(),
			logger,
			config.otlpEndpoint,
			!config.disableOtlpTraces,
			!config.disableOtlpMetrics,
			config.traceEverything)
	if err != nil {
		logger.Error("failed to initialize opentelemetry tracing", zap.Error(err))
		os.Exit(1)
	}

	if otlpTracerProvider != nil {
		otel.SetTracerProvider(otlpTracerProvider)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	}
	if otlpMeterProvider != nil {
		otel.SetMeterProvider(otlpMeterProvider)
	}

	// setup the web service
	webListenAddress := fmt.Sprintf("%s:%v", config.bindAddress, config.webPort)
	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        logger,
		LogLevel:      &logLevel,
		ListenAddress: webListenAddress,
	})

	var selfSignedCert *tls.Certificate
	if config.selfSign {
		generatedCert, err := selfsignedcert.GenerateCertificate()
		if err != nil {
			logger.Error("failed to generate a self-signed certificate")
			os.Exit(1)
		}

		selfSignedCert = generatedCert
	}

	var grpcCertificate tls.Certificate
	if config.dataPort != -1 || config.sdPort != -1 {
		// GRPC services are enabled
		grpcCertPath := config.grpcCertPath
		if grpcCertPath == "" {
			grpcCertPath = config.certPath
		}

		grpcKeyPath := config.grpcKeyPath
		if grpcKeyPath == "" {
			grpcKeyPath = config.keyPath
		}

		if grpcCertPath == "" || grpcKeyPath == "" {
			if selfSignedCert == nil {
				logger.Error("must specify both grpc-cert/grpc-key or cert/key unless self-sign is specified")
				os.Exit(1)
			}

			grpcCertificate = *selfSignedCert
		} else {
			loadedTlsCertificate, err := tls.LoadX509KeyPair(grpcCertPath, grpcKeyPath)
			if err != nil {
				logger.Error("failed to load tls certificate", zap.Error(err))
				os.Exit(1)
			}

			grpcCertificate = loadedTlsCertificate
		}
	}

	var dapiCertificate tls.Certificate
	if config.dapiPort != -1 {
		// Data API service is enabled
		dapiCertPath := config.dapiCertPath
		if dapiCertPath == "" {
			dapiCertPath = config.certPath
		}

		dapiKeyPath := config.dapiKeyPath
		if dapiKeyPath == "" {
			dapiKeyPath = config.keyPath
		}

		if dapiCertPath == "" || dapiKeyPath == "" {
			if selfSignedCert == nil {
				logger.Error("must specify both dapi-cert/dapi-key or cert/key unless self-sign is specified")
				os.Exit(1)
			}

			dapiCertificate = *selfSignedCert
		} else {
			loadedTlsCertificate, err := tls.LoadX509KeyPair(dapiCertPath, dapiKeyPath)
			if err != nil {
				logger.Error("failed to load tls certificate", zap.Error(err))
				os.Exit(1)
			}

			dapiCertificate = loadedTlsCertificate
		}
	}

	if config.cbCredsAwsId != "" {
		if config.cbUser != "Administrator" || config.cbPass != "password" {
			logger.Error("cannot use cb-pass or cb-user when fetching creds from cloud provider")
			os.Exit(1)
		}

		if config.cbCredsAwsRegion == "" {
			logger.Error("must specify region and id when fetching secrets from aws")
			os.Exit(1)
		}

		logger.Info("fetching server credentials from aws secrets manager")
		config.cbUser, config.cbPass, err = secretsmanager.FetchAWSSecret(config.cbCredsAwsId, config.cbCredsAwsRegion)

		if err != nil {
			logger.Error("failed to fetch couchbase server password from aws", zap.Error(err))
			os.Exit(1)
		}
	}

	if config.cbCredsAzureId != "" {
		if config.cbUser != "Administrator" || config.cbPass != "password" {
			logger.Error("cannot use cb-pass or cb-user when fetching creds from cloud provider")
			os.Exit(1)
		}

		if config.cbCredsAzureVaultName == "" {
			logger.Error("must specify key vault name and id when fetching secrets from azure")
			os.Exit(1)
		}

		logger.Info("fetching server credentials from azure key vault")
		config.cbUser, config.cbPass, err = secretsmanager.FetchAzureSecret(config.cbCredsAzureId, config.cbCredsAzureVaultName)

		if err != nil {
			logger.Error("failed to fetch couchbase server password from azure", zap.Error(err))
			os.Exit(1)
		}
	}

	if config.cbCredsGcpId != "" {
		if config.cbUser != "Administrator" || config.cbPass != "password" {
			logger.Error("cannot use cb-pass or cb-user when fetching creds from cloud provider")
			os.Exit(1)
		}

		if config.cbCredsGcpProjectId == "" {
			logger.Error("must specify project and secret ids when fetching secrets from gcp")
			os.Exit(1)
		}

		logger.Info("fetching server credentials from gcp secrets manager")
		config.cbUser, config.cbPass, err = secretsmanager.FetchGcpSecret(config.cbCredsGcpId, config.cbCredsGcpProjectId)

		if err != nil {
			logger.Error("failed to fetch couchbase server password from gcp", zap.Error(err))
			os.Exit(1)
		}
	}

	gatewayConfig := &gateway.Config{
		Logger:          logger.Named("gateway"),
		CbConnStr:       config.cbHost,
		Username:        config.cbUser,
		Password:        config.cbPass,
		Daemon:          daemon,
		Debug:           config.debug,
		ProxyServices:   strings.Split(config.dapiProxyServices, ","),
		BindDataPort:    config.dataPort,
		BindSdPort:      config.sdPort,
		BindDapiPort:    config.dapiPort,
		BindAddress:     config.bindAddress,
		RateLimit:       config.rateLimit,
		GrpcCertificate: grpcCertificate,
		DapiCertificate: dapiCertificate,
		NumInstances:    1,
		StartupCallback: func(m *gateway.StartupInfo) {
			webapi.MarkSystemHealthy()
		},
	}

	gw, err := gateway.NewGateway(gatewayConfig)
	if err != nil {
		logger.Error("failed to initialize the gateway", zap.Error(err))
		os.Exit(1)
	}

	var configLock sync.Mutex
	reloadConfiguration := func() {
		configLock.Lock()
		defer configLock.Unlock()

		err := viper.ReadInConfig()
		if err != nil {
			logger.Warn("failed to parse configuration file",
				zap.Error(err))
		}

		newConfig := readConfig(logger)

		if newConfig.cbHost != config.cbHost ||
			newConfig.cbUser != config.cbUser ||
			newConfig.cbPass != config.cbPass {
			logger.Warn("config changes for cbHost, cbUser, or cbPass require a restart")
		}

		if newConfig.bindAddress != config.bindAddress ||
			newConfig.dataPort != config.dataPort ||
			newConfig.sdPort != config.sdPort ||
			newConfig.dapiPort != config.dapiPort {
			logger.Warn("config changes for bindAddress, dataPort, sdPort, or dapiPort require a restart")
		}

		if newConfig.selfSign != config.selfSign {
			logger.Warn("config changes for selfSign require a restart")
		}

		if newConfig.certPath != config.certPath ||
			newConfig.keyPath != config.keyPath ||
			newConfig.grpcCertPath != config.grpcCertPath ||
			newConfig.grpcKeyPath != config.grpcKeyPath ||
			newConfig.dapiCertPath != config.dapiCertPath ||
			newConfig.dapiKeyPath != config.dapiKeyPath {
			logger.Warn("config changes for certPath, keyPath, grpcCertPath, grpcKeyPath, dapiCertPath, or dapiKeyPath require a restart")
		}

		if newConfig.otlpEndpoint != config.otlpEndpoint ||
			newConfig.disableOtlpTraces != config.disableOtlpTraces ||
			newConfig.disableOtlpMetrics != config.disableOtlpMetrics ||
			newConfig.traceEverything != config.traceEverything {
			logger.Warn("config changes for otlpEndpoint, disableOtlpTraces, disableOtlpMetrics, or traceEverything require a restart")
		}

		if newConfig.debug != config.debug {
			logger.Warn("config changes for debug require a restart")
		}

		if newConfig.cpuprofile != config.cpuprofile {
			logger.Warn("config changes for cpuprofile require a restart")
		}

		if newConfig.dapiProxyServices != config.dapiProxyServices {
			logger.Warn("config changes for dapiProxyServices require a restart")
		}

		if newConfig.logLevelStr != config.logLevelStr {
			newParsedLogLevel, err := zapcore.ParseLevel(newConfig.logLevelStr)
			if err != nil {
				logger.Warn("invalid log level specified, using INFO instead")
				newParsedLogLevel = zapcore.InfoLevel
			}

			logLevel.SetLevel(newParsedLogLevel)

			logger.Info("updated log level",
				zap.String("newLevel", newParsedLogLevel.String()))
		}

		if newConfig.rateLimit != config.rateLimit {
			err := gw.Reconfigure(&gateway.ReconfigureOptions{
				RateLimit: newConfig.rateLimit,
			})
			if err != nil {
				logger.Warn("failed to reconfigure system", zap.Error(err))
			}
		}

		config = newConfig
	}

	if watchCfgFile {
		viper.OnConfigChange(func(in fsnotify.Event) {
			logger.Info("configuration file change detected")
			reloadConfiguration()
		})

		go viper.WatchConfig()
	}

	go func() {
		sigCh := make(chan os.Signal, 10)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

		beginGracefulShutdown := func() {
			gw.Shutdown()
		}

		hasReceivedSigInt := false
		for sig := range sigCh {
			if sig == syscall.SIGINT {
				if hasReceivedSigInt {
					logger.Info("Received SIGINT a second time, terminating...")
					os.Exit(1)
				} else {
					logger.Info("Received SIGINT, attempting graceful shutdown...")
					hasReceivedSigInt = true
					beginGracefulShutdown()
				}
			} else if sig == syscall.SIGTERM {
				logger.Info("Received SIGTERM, attempting graceful shutdown...")
				beginGracefulShutdown()
			} else if sig == syscall.SIGHUP {
				logger.Info("Received SIGHUP, reloading configuration...")
				reloadConfiguration()
			}
		}
	}()

	err = gw.Run(context.Background())
	if err != nil {
		logger.Error("failed to run the gateway", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("gateway shutdown gracefully")
}

func startGatewayWatchdog() {
	_, logger := getLogger()
	logger = logger.Named("watchdog")

	execProc := os.Args[0]
	execArgs := append([]string{"--auto-restart-proc"}, os.Args[1:]...)

	hasReceivedSigInt := false
	go func() {
		sigCh := make(chan os.Signal, 10)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		for sig := range sigCh {
			if sig == syscall.SIGINT {
				if hasReceivedSigInt {
					logger.Info("received sigint a second time, terminating...")
					os.Exit(1)
				} else {
					logger.Info("received sigint, waiting for graceful shutdown...")
					hasReceivedSigInt = true
				}
			} else if sig == syscall.SIGTERM {
				logger.Info("received sigterm, waiting for graceful shutdown...")
			}
		}
	}()

	for {
		logger.Info("starting sub-process")

		cmd := exec.Command(execProc, execArgs...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		err := cmd.Start()
		if err != nil {
			logger.Info("failed to start sub-process", zap.Error(err))
		}

		err = cmd.Wait()
		if err != nil {
			logger.Info("sub-process exited with error", zap.Error(err))
		}

		if hasReceivedSigInt {
			break
		}

		delayTime := 1 * time.Second
		logger.Info("crash detected, restarting", zap.Duration("delay", delayTime))
		time.Sleep(delayTime)
	}
}

func main() {
	cobra.CheckErr(rootCmd.Execute())
}
