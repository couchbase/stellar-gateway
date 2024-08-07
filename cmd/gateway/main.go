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
	configFlags.Bool("self-sign", false, "specifies to use a self-signed certificate rather than specifying them")
	configFlags.String("cert", "", "path to server tls cert")
	configFlags.String("key", "", "path to server private tls key")
	configFlags.String("cacert", "", "path to root CA cert")
	configFlags.String("otlp-endpoint", "", "opentelemetry endpoint to send telemetry to")
	configFlags.Bool("disable-otlp-traces", false, "disable sending traces to otlp")
	configFlags.Bool("disable-otlp-metrics", false, "disable sending metrics to otlp")
	configFlags.Bool("trace-everything", false, "enables tracing of all components")
	configFlags.Bool("debug", false, "enable debug mode")
	configFlags.String("cpuprofile", "", "write cpu profile to a file")
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

	logLevelStr := viper.GetString("log-level")
	cbHost := viper.GetString("cb-host")
	cbUser := viper.GetString("cb-user")
	cbPass := viper.GetString("cb-pass")
	bindAddress := viper.GetString("bind-address")
	dataPort := viper.GetInt("data-port")
	sdPort := viper.GetInt("sd-port")
	webPort := viper.GetInt("web-port")
	dapiPort := viper.GetInt("dapi-port")
	selfSign := viper.GetBool("self-sign")
	certPath := viper.GetString("cert")
	keyPath := viper.GetString("key")
	caCertPath := viper.GetString("cacert")
	otlpEndpoint := viper.GetString("otlp-endpoint")
	disableOtlpTraces := viper.GetBool("disable-otlp-traces")
	disableOtlpMetrics := viper.GetBool("disable-otlp-metrics")
	traceEverything := viper.GetBool("trace-everything")
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
		zap.Int("dapiPort", dapiPort),
		zap.Bool("selfSign", selfSign),
		zap.String("certPath", certPath),
		zap.String("keyPath", keyPath),
		zap.String("cacertPath", caCertPath),
		zap.String("otlpEndpoint", otlpEndpoint),
		zap.Bool("disableOtlpTraces", disableOtlpTraces),
		zap.Bool("disableOtlpMetrics", disableOtlpMetrics),
		zap.Bool("traceEverything", traceEverything),
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
		initTelemetry(context.Background(),
			logger,
			otlpEndpoint,
			!disableOtlpTraces,
			!disableOtlpMetrics,
			traceEverything)
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
		BindDapiPort:   dapiPort,
		BindAddress:    bindAddress,
		TlsCertificate: tlsCertificate,
		NumInstances:   1,
		StartupCallback: func(m *gateway.StartupInfo) {
			webapi.MarkSystemHealthy()
		},
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
