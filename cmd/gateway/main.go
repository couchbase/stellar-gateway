package main

import (
	"context"
	"fmt"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/pkg/app_config"
	"github.com/couchbase/stellar-gateway/pkg/webapi"
)


func main() {

	mainConfig := app_config.SetupConfig()

	listenAddress := fmt.Sprintf("0.0.0.0:%v", mainConfig.Config.WebPort)

	webapi.InitializeWebServer(webapi.WebServerOptions{
		Logger:        mainConfig.Logger.Named("webapi"),
		ListenAddress: listenAddress,
	})



	gateway.Run(context.Background(), mainConfig)
}
