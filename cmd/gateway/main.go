package main

import (
	"context"
	"fmt"
	"os"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/gateway/app_config"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
	"go.uber.org/zap"
)

func main() {

	config := app_config.SetupConfig()

	// In order to start the bridge, we need to know where the gateway is running,
	// so we use a channel and a hook in the gateway to get this.
	listenAddress := fmt.Sprintf("0.0.0.0:%v", config.Config.WebPort)

	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        config.Logger.Named("web-api"),
		ListenAddress: listenAddress,
	})

	err := gateway.Run(context.Background(), config)
	if err != nil {
		config.Logger.Error("failed to initialize the gateway", zap.Error(err))
		os.Exit(1)
	}
}
