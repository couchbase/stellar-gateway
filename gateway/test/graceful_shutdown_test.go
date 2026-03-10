package test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// A query that takes a couple of minutes to complete
const slowQuery = "SELECT SUM(t + t2) AS total FROM ARRAY_RANGE(0,8000) AS t, ARRAY_RANGE(0,4000) AS t2"

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
		ProxyServices:   []string{"query"},
		StartupCallback: func(m *gateway.StartupInfo) {
			gwStartInfoCh <- m
		},
		ShutdownTimeout: time.Second * 10,
	})
	if err != nil {
		s.T().Fatalf("failed to initialize graceful-shutdown-gateway: %s", err)
	}

	var wg sync.WaitGroup

	gwClosedCh := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	respCloseChan := make(chan (bool), 10000)

	// Test that a client attempting to do work against data API during shutdown will see keep alives disabled and then
	// connection refused errors.
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

			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			respCloseChan <- resp.Close
			time.Sleep(time.Millisecond * 10)
		}
	}()

	// Check that requests to Data API taking longer than the shutdown timeout will eventually be forcibly closed.
	wg.Add(1)
	slowHttpReqSent := make(chan any)
	go func() {
		defer wg.Done()

		statement := []byte(fmt.Sprintf(`{"statement": "%s"}`, slowQuery))
		reqBody := bytes.NewReader(statement)

		req, err := http.NewRequestWithContext(context.Background(), "POST", fmt.Sprintf("https://%s/_p/query/query/service", dapiAddr), reqBody)
		require.NoError(s.T(), err, "failed to create request with client trace")

		req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)
		req.Header.Set("Content-Type", "application/json")

		slowHttpReqSent <- struct{}{}
		_, err = dapiCli.Do(req)

		// After the shutdown timeout has elapsed the gateway should forcibly close connections, causing the client to see an
		// EOF error.
		assert.ErrorIs(s.T(), err, io.EOF)
	}()

	// Check that grpc requests that take longer than the shutdown timeout will eventually be forcibly closed.
	slowGrpcReqSent := make(chan any)
	wg.Add(1)
	go func() {
		defer wg.Done()
		connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.PS)
		conn, err := grpc.NewClient(connAddr,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
			grpc.WithUserAgent("test-client"),
		)
		defer func() { _ = conn.Close() }()
		assert.NoError(s.T(), err, "failed to create grpc client connection to gateway")

		queryClient := query_v1.NewQueryServiceClient(conn)

		slowGrpcReqSent <- struct{}{}
		client, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
			BucketName: &s.bucketName,
			Statement:  slowQuery,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assert.NoError(s.T(), err, "failed to send grpc query request")

		_, err = client.Recv()
		assert.Error(s.T(), err, "expected error receiving from grpc stream after shutdown")

		st, ok := status.FromError(err)
		assert.True(s.T(), ok)
		assert.Equal(s.T(), codes.Unavailable, st.Code())
		assert.Contains(s.T(), st.Message(), "EOF")
	}()

	// Wait for the slow running request to be sent before starting shutdown
	<-slowHttpReqSent
	<-slowGrpcReqSent

	// Allow the server to start processing the requests
	time.Sleep(time.Second * 5)

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
