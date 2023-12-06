package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
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
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var buildVersion string = buildversion.GetVersion("github.com/couchbase/stellar-gateway")

var rootCmd = &cobra.Command{
	Version: buildVersion,

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
	configFlags.String("otlp-endpoint", "", "opentelemetry endpoint to send telemetry to")
	configFlags.Bool("debug", false, "enable debug mode")
	configFlags.String("cpuprofile", "", "write cpu profile to a file")
	rootCmd.Flags().AddFlagSet(configFlags)

	_ = viper.BindPFlags(configFlags)
	viper.SetEnvPrefix("stg")
	viper.AutomaticEnv()
}

func initTelemetry(
	ctx context.Context,
	logger *zap.Logger,
	otlpEndpoint string,
) (
	trace.TracerProvider,
	metric.MeterProvider,
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

	if otlpEndpoint == "" {
		meterProvider := sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(promExp),
		)

		return nil, meterProvider, nil
	}

	metricExp, err := otlpmetricgrpc.New(
		ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(otlpEndpoint))
	if err != nil {
		return nil, nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				metricExp,
				sdkmetric.WithInterval(2*time.Second),
			),
		),
		sdkmetric.WithReader(promExp),
	)

	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithDialOption(grpc.WithBlock()))
	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		return nil, nil, err
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	return tracerProvider, meterProvider, nil
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
	otlpEndpoint := viper.GetString("otlp-endpoint")
	debug := viper.GetBool("debug")
	cpuprofile := viper.GetString("cpuprofile")

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
		zap.String("otlpEndpoint", otlpEndpoint),
		zap.Bool("debug", debug),
		zap.String("cpuprofile", cpuprofile),
	)

	parsedLogLevel, err := zapcore.ParseLevel(logLevelStr)
	if err != nil {
		logger.Warn("invalid log level specified, using INFO instead")
		parsedLogLevel = zapcore.InfoLevel
	}
	logLevel.SetLevel(parsedLogLevel)

	// setup profiling
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
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
		initTelemetry(context.Background(), logger, otlpEndpoint)
	if err != nil {
		logger.Error("failed to initialize opentelemetry tracing", zap.Error(err))
		os.Exit(1)
	}

	if otlpTracerProvider != nil {
		otel.SetTracerProvider(otlpTracerProvider)
	}
	if otlpMeterProvider != nil {
		otel.SetMeterProvider(otlpMeterProvider)
	}

	// setup the web service
	webListenAddress := fmt.Sprintf("%s:%v", bindAddress, webPort)
	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        logger,
		LogLevel:      &logLevel,
		ListenAddress: webListenAddress,
	})

	var tlsCertificate tls.Certificate
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
		Debug:          debug,
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

	go func() {
		sigCh := make(chan os.Signal, 10)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

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

func main() {
	cobra.CheckErr(rootCmd.Execute())
}
