package test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func (s *GatewayOpsTestSuite) TestGracefulShutdown() {
	s.T().Logf("setting up new instance of stellar gateway...")

	gwCert, err := selfsignedcert.GenerateCertificate()
	if err != nil {
		s.T().Fatalf("failed to create testing certificate: %s", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		s.T().Fatalf("failed to initialize test logging: %s", err)
	}

	testConfig := testutils.GetTestConfig(s.T())

	gwStartInfoCh := make(chan *gateway.StartupInfo, 1)
	gw, err := gateway.NewGateway(&gateway.Config{
		Logger:          logger.Named("graceful-shutdown-gateway"),
		CbConnStr:       testConfig.CbConnStr,
		Username:        testConfig.CbUser,
		Password:        testConfig.CbPass,
		GrpcCertificate: *gwCert,
		DapiCertificate: *gwCert,
		NumInstances:    1,

		StartupCallback: func(m *gateway.StartupInfo) {
			gwStartInfoCh <- m
		},
	})
	if err != nil {
		s.T().Fatalf("failed to initialize graceful-shutdown-gateway: %s", err)
	}

	gwClosedCh := make(chan struct{})
	go func() {
		err := gw.Run(context.Background())
		if err != nil {
			s.T().Errorf("graceful-shutdown-gateway run failed: %s", err)
		}

		s.T().Logf("graceful-shutdown-gateway has shut down")
		close(gwClosedCh)
	}()

	startInfo := <-gwStartInfoCh

	dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.AdvertisePorts.DAPI)
	dapiCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	respCloseChan := make(chan (bool), 10000)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			resp, err := dapiCli.Get(fmt.Sprintf("https://%s/v1/callerIdentity", dapiAddr))
			if err != nil {
				// A non-nil error should be caused by sending requests to the gateway
				// after it has already shutdown.
				assert.ErrorIs(s.T(), err, syscall.ECONNREFUSED)
				return
			}

			respCloseChan <- resp.Close
			time.Sleep(time.Millisecond * 10)
		}
	}()

	// Allow some requests to run against the gateway before shutting down
	time.Sleep(time.Second)

	gw.Shutdown()

	wg.Wait()

	isFirstResponse := true
	keepAlivesDisabled := false
	for len(respCloseChan) > 0 {
		respClose := <-respCloseChan
		if isFirstResponse {
			// Since the gateway is always healthy for the first request resp.Close should be false
			assert.False(s.T(), respClose)
			isFirstResponse = false
		}

		// If graceful shutdown is working correctly some requests should see resp.Close = true
		keepAlivesDisabled = respClose || keepAlivesDisabled
	}

	assert.True(s.T(), keepAlivesDisabled)
}
