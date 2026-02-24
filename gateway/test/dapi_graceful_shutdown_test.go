package test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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

	dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.DAPI)
	dapiCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s/v1/callerIdentity", dapiAddr), nil)
	assert.NoError(s.T(), err)

	req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)

	numWorkers := 10
	var requestsStarted sync.WaitGroup
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		requestsStarted.Add(1)
		wg.Add(1)
		clonedReq := req.Clone(req.Context())

		go func() {
			defer wg.Done()
			firstRequest := true
			for {
				resp, err := dapiCli.Do(clonedReq)
				if err != nil {
					assert.ErrorIs(s.T(), err, syscall.ECONNREFUSED)
					break
				}

				if firstRequest {
					requestsStarted.Done()
					firstRequest = false
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				err = resp.Body.Close()
				assert.NoError(s.T(), err)

				assert.True(s.T(), resp.StatusCode == http.StatusOK)
				time.Sleep(time.Millisecond * 10)
			}
		}()
	}

	// Wait for all clients to connect before shutting down
	requestsStarted.Wait()

	gw.Shutdown()

	wg.Wait()
}
