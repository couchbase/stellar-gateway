package test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
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

	var requestsWritten atomic.Int64
	var responsesReceived atomic.Int64
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			requestsWritten.Add(1)
		},
		GotFirstResponseByte: func() {
			responsesReceived.Add(1)
		},
	}

	ctx := httptrace.WithClientTrace(context.Background(), trace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://%s/v1/callerIdentity", dapiAddr), nil)
	assert.NoError(s.T(), err)

	respCloseChan := make(chan (bool), 10000)
	var wg sync.WaitGroup

	var eofs int
	var unexpectedErr error
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			resp, err := dapiCli.Do(req)
			if err != nil {
				switch {
				case errors.Is(err, io.EOF):
					// There is a small window between the flushing the request bytes to the socket and the http handler receiving
					// the bytes where the server sees the connection as idle and will close it. We record these errors and
					// include them when checking all requests recieved responses.
					eofs++
				case errors.Is(err, syscall.ECONNREFUSED):
					// This is what we expect to see once the listeners have closed
				case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
					// Connection reset and broken pipe errors occur before the request is completely written, therefore the
					// wroteRequest hook has not triggered. Such racy errors are unavoidable when running directly against an
					// http server.
				default:
					// Any errors not mentinoned above are not expected and should cause a failure to be investigated.
					unexpectedErr = err
					return
				}

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

	assert.NoError(s.T(), unexpectedErr)
	assert.Equal(s.T(), requestsWritten.Load(), responsesReceived.Load()+int64(eofs))

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
