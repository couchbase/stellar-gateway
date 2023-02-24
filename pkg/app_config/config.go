package app_config

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/pkg/version"
	"go.uber.org/zap"
)

const (
	ClientTlsEnvVar   = "CB_SNG_CLIENT_TLS_PATH"
	ServerTlsEnvVar   = "CB_SNG_SERVER_TLS_PATH"
	CredentialsEnvVar = "CB_SNG_CREDENTIALS_PATH"
	ConfigEnvVar      = "CB_SNG_CONFIG_PATH"
)

var (
	cbHost     = flag.String("cb-host", "127.0.0.1", "the couchbase server host")
	cbUser     = flag.String("cb-user", "Administrator", "the couchbase server username")
	cbPass     = flag.String("cb-pass", "password", "the couchbase server password")
	dataPort   = flag.Int("data-port", 18098, "the data port")
	sdPort     = flag.Int("sd-port", 18099, "the sd port")
	webPort    = flag.Int("web-port", 9091, "the web metrics/health port")
	daemon     = flag.Bool("daemon", false, "When in daemon mode, stellar-gateway will restart on failure")
	clientCert = flag.String("client-cert", "", "Couchbase Client certificate for mtls ")
	clientKey  = flag.String("client-key", "", "Couchbase client key for mtls")
	serverCert = flag.String("server-cert", "", "Certificate for TLS for external hosted elements")
	serverKey  = flag.String("server-key", "", "Key for TLS for external hosted elements")
)

type TlsConfig struct {
	Crt string `json:"crt"`
	Key string `json:"key"`
}

type CredentialsConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type GeneralConfig struct {
	LogLevel         string `json:"logLevel"`
	DataPort         int    `json:"dataPort"`
	SdPort           int    `json:"sdPort"`
	WebPort          int    `json:"webPort"`
	Daemon           bool   `json:"daemon"`
	ConnectionString string `json:"connectionString"`
	BindAddress      string `json:"bindAddress"`
}

type Config struct {
	Logger      *zap.Logger
	NodeID      string
	ServerGroup string

	Config          *GeneralConfig
	ClientTLSConfig *TlsConfig
	ServerTLSConfig *TlsConfig
	Credentials     *CredentialsConfig

	ConfigWatcher      *ConfigWatcher[GeneralConfig]
	ClientTLSWatcher   *ConfigWatcher[TlsConfig]
	ServerTLSWatcher   *ConfigWatcher[TlsConfig]
	CredentialsWatcher *ConfigWatcher[CredentialsConfig]

	AdvertiseAddress string
	AdvertisePorts   ServicePorts

	NumInstances    uint
	StartupCallback func(*StartupInfo)

	SnMetrics *metrics.SnMetrics
}

type StartupInfo struct {
	MemberID       string
	ServerGroup    string
	AdvertiseAddr  string
	AdvertisePorts ServicePorts
}

type ServicePorts struct {
	PS int `json:"p,omitempty"`
	SD int `json:"s,omitempty"`
}

func SetupConfig() *Config {
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Printf("failed to initialize logging: %s", err)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Starting %s: %s", version.Application, version.WithBuildNumberAndRevision()))

	// In order to start the bridge, we need to know where the gateway is running,
	// so we use a channel and a hook in the gateway to get this.

	// Todo:  Read in log level from CLI or env var

	gatewayConnStrCh := make(chan string, 100)

	gatewayConfig := &Config{
		Logger: logger.Named("gateway"),
		Config: &GeneralConfig{
			WebPort:          *webPort,
			ConnectionString: *cbHost,
			Daemon:           *daemon,
			DataPort:         *dataPort,
			SdPort:           *sdPort,
			BindAddress:      "0.0.0.0",
		},
		Credentials: &CredentialsConfig{
			Username: *cbUser,
			Password: *cbPass,
		},
		ServerTLSConfig: &TlsConfig{
			Crt: *serverCert,
			Key: *serverKey,
		},
		ClientTLSConfig: &TlsConfig{
			Crt: *clientCert,
			Key: *clientKey,
		},

		NumInstances: 1,
		SnMetrics:    metrics.GetSnMetrics(),

		StartupCallback: func(m *StartupInfo) {
			gatewayConnStrCh <- fmt.Sprintf("%s:%d", m.AdvertiseAddr, m.AdvertisePorts.PS)
		},
	}

	if configPath, ok := os.LookupEnv(ConfigEnvVar); ok && configPath != "" {
		logger.Info("Config path found.", zap.String("path", configPath))
		err := readFileAndUnmarshal(configPath, gatewayConfig.Config)
		if err != nil {
			logger.Error("Error parsing config file.")
		}
		if gatewayConfig.Config.LogLevel != "" {
			gatewayConfig.Logger = getLogger(gatewayConfig.Config.LogLevel)
		}
		gatewayConfig.ConfigWatcher = NewConfigWatcher[GeneralConfig](configPath)

		// now we can watch the config for log changes
		watchForLogLevelChanges(gatewayConfig)
	}

	if clientTlsConfigPath, ok := os.LookupEnv(ClientTlsEnvVar); ok && clientTlsConfigPath != "" {
		logger.Info("Client tls config path found.", zap.String("path", clientTlsConfigPath))
		err := readFileAndUnmarshal(clientTlsConfigPath, gatewayConfig.ClientTLSConfig)
		if err != nil {
			logger.Error("Error parsing Client TLS Config file.")
		}
		gatewayConfig.ClientTLSWatcher = NewConfigWatcher[TlsConfig](clientTlsConfigPath)
	}

	if serverTlsConfigPath, ok := os.LookupEnv(ServerTlsEnvVar); ok && serverTlsConfigPath != "" {
		logger.Info("Server tls config path found.", zap.String("path", serverTlsConfigPath))
		err := readFileAndUnmarshal(serverTlsConfigPath, gatewayConfig.ServerTLSConfig)
		if err != nil {
			logger.Error("Error parsing Server TLS Config file.")
		}
		gatewayConfig.ServerTLSWatcher = NewConfigWatcher[TlsConfig](serverTlsConfigPath)
	}

	if credentialsConfigPath, ok := os.LookupEnv(CredentialsEnvVar); ok && credentialsConfigPath != "" {
		logger.Info("Credentials config path found.", zap.String("path", credentialsConfigPath))
		err := readFileAndUnmarshal(credentialsConfigPath, gatewayConfig.Credentials)
		if err != nil {
			logger.Error("Error parsing Credentials Config file.")
		}
		gatewayConfig.CredentialsWatcher = NewConfigWatcher[CredentialsConfig](credentialsConfigPath)
	}

	return gatewayConfig
}

func readFileAndUnmarshal[T any](path string, target *T) error {
	var err error
	if _, err = os.Stat(path); err == nil {
		bytes, _ := os.ReadFile(path)
		err = json.Unmarshal(bytes, target)
	} 
	return err
}

func getLogger(logLevel string) *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Printf("failed to initialize info logging: %s", err)
		os.Exit(1)
	}
	if strings.EqualFold("debug", logLevel) {
		logger, err = getDebugLogger()
		logger.Debug("changing to debug logger")
		if err != nil {
			log.Printf("failed to initialize debug logging: %s", err)
			os.Exit(1)
		}
	}
	return logger
}

func getDebugLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	return cfg.Build()
}

func watchForLogLevelChanges(gatewayConfig *Config) {
	ch := make(chan GeneralConfig)
	unsub := gatewayConfig.ConfigWatcher.Subscribe(ch)
	go func() {
		defer unsub()
		for {
			c := <-ch
			gatewayConfig.Logger.Info("Configuration change detected... Updating")
			gatewayConfig.Config = &c
			if c.LogLevel != "" {
				gatewayConfig.Logger = getLogger(c.LogLevel)
			}
		}
	}()
}
