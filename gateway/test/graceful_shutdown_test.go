package test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const shutDownTimeout = 20 * time.Second

func (s *GatewayOpsTestSuite) setupKvGetBarrierHook(ctx context.Context, conn *grpc.ClientConn, hooksContextID, barrierID string) {
	hooksClient := internal_hooks_v1.NewHooksServiceClient(conn)

	_, err := hooksClient.CreateHooksContext(context.Background(), &internal_hooks_v1.CreateHooksContextRequest{
		Id: hooksContextID,
	})
	require.NoError(s.T(), err, "failed to create hooks context")

	_, err = hooksClient.AddHooks(context.Background(), &internal_hooks_v1.AddHooksRequest{
		HooksContextId: hooksContextID,
		Hooks: []*internal_hooks_v1.Hook{
			{
				Name:         "block-kv-get",
				Description:  "Block KV Get on a barrier to simulate a long-running request",
				TargetMethod: "/couchbase.kv.v1.KvService/Get",
				Actions: []*internal_hooks_v1.HookAction{
					{
						Action: &internal_hooks_v1.HookAction_WaitOnBarrier_{
							WaitOnBarrier: &internal_hooks_v1.HookAction_WaitOnBarrier{
								BarrierId: barrierID,
							},
						},
					},
					{
						Action: &internal_hooks_v1.HookAction_Execute_{
							Execute: &internal_hooks_v1.HookAction_Execute{},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err, "failed to add hooks")
}

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
		Debug:           true,
		StartupCallback: func(m *gateway.StartupInfo) {
			gwStartInfoCh <- m
		},
		ShutdownTimeout: shutDownTimeout,
	})
	if err != nil {
		s.T().Fatalf("failed to initialize graceful-shutdown-gateway: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := gw.Run(context.Background())
		if err != nil {
			s.T().Errorf("graceful-shutdown-gateway run failed: %s", err)
		}

		s.T().Logf("graceful-shutdown-gateway has shut down")
	}()

	startInfo := <-gwStartInfoCh

	dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.DAPI)
	dapiCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	eofDapiHooksContextID := "http-eof-test-case"
	err = startInfo.HooksManager.CreateHooksContext(eofDapiHooksContextID)
	require.NoError(s.T(), err, "failed to create http eof hooks context")

	// Check that requests to Data API taking longer than the shutdown timeout will eventually be forcibly closed.
	wg.Add(1)
	eofDapiReqSent := make(chan any)
	go func() {
		defer wg.Done()

		statement := []byte(fmt.Sprintf(`{"statement": "%s"}`, "SELECT 1 == 1"))
		reqBody := bytes.NewReader(statement)

		req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/_p/query/query/service", dapiAddr), reqBody)
		require.NoError(s.T(), err, "failed to create http eof request")

		req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Hooks-ID", eofDapiHooksContextID)
		req.Header.Set("X-Barrier-ID", "eof-barrier")

		eofDapiReqSent <- struct{}{}
		_, err = dapiCli.Do(req)

		// After the shutdown timeout has elapsed the gateway should forcibly close connections, causing the client to see an
		// EOF error.
		assert.ErrorIs(s.T(), err, io.EOF)
	}()

	successDapiHooksContextID := "http-success-test-case"
	err = startInfo.HooksManager.CreateHooksContext(successDapiHooksContextID)
	require.NoError(s.T(), err, "failed to create http success hooks context")

	// Check that requests that start before shutdown is called but take less than the shutdown timeout complete successfully.
	wg.Add(1)
	successDapiReqSent := make(chan any)
	go func() {
		defer wg.Done()

		statement := []byte(fmt.Sprintf(`{"statement": "%s"}`, "SELECT 1 == 1"))
		reqBody := bytes.NewReader(statement)

		req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/_p/query/query/service", dapiAddr), reqBody)
		require.NoError(s.T(), err, "failed to create http success request")

		req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Hooks-ID", successDapiHooksContextID)
		req.Header.Set("X-Barrier-ID", "success-barrier")

		successDapiReqSent <- struct{}{}
		resp, err := dapiCli.Do(req)

		assert.NoError(s.T(), err, "expected request to complete successfully before shutdown timeout")

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.PS)
	conn, err := grpc.NewClient(connAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
		grpc.WithUserAgent("test-client"),
	)
	require.NoError(s.T(), err, "failed to create grpc client connection to gateway")
	defer func() { _ = conn.Close() }()

	testKey := s.testDocId()

	// The call to setupKvGetBarrierHook can hang indefinitely if the hooks service is unreachable so we use a context with a
	// generous timeout
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Check that grpc requests that take longer than the shutdown timeout will eventually be forcibly closed.
	eofGrpcReqSent := make(chan any)
	wg.Add(1)
	go func() {
		defer wg.Done()

		kvClient := kv_v1.NewKvServiceClient(conn)

		// Setup a barrier hook that blocks the request permanently
		hooksContextID := "eof-test-case"
		s.setupKvGetBarrierHook(ctx, conn, hooksContextID, "eof-barrier")

		grpcCtx := metadata.AppendToOutgoingContext(context.Background(), "X-Hooks-ID", hooksContextID)
		eofGrpcReqSent <- struct{}{}
		_, err := kvClient.Get(grpcCtx, &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            testKey,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))

		assert.Error(s.T(), err, "expected error from grpc request after forcible shutdown")

		st, ok := status.FromError(err)
		assert.True(s.T(), ok)
		assert.Equal(s.T(), codes.Unavailable, st.Code())
	}()

	// Signal the barriers after a delay to allow requests to be in-flight when shutdown is called, but unblock them
	// before the shutdown timeout elapses so that they can complete successfully.
	hooksContextID := "success-test-case"
	s.setupKvGetBarrierHook(ctx, conn, hooksContextID, "temp-barrier")

	time.AfterFunc(shutDownTimeout-time.Second*5, func() {
		hooksCtx := startInfo.HooksManager.GetHooksContext(hooksContextID)
		if hooksCtx != nil {
			hooksCtx.GetBarrier("temp-barrier").SignalAll(nil)
		}

		httpHooksCtx := startInfo.HooksManager.GetHooksContext(successDapiHooksContextID)
		if httpHooksCtx != nil {
			httpHooksCtx.GetBarrier("success-barrier").SignalAll(nil)
		}
	})

	successGrpcReqSent := make(chan any)
	wg.Add(1)
	go func() {
		defer wg.Done()

		kvClient := kv_v1.NewKvServiceClient(conn)

		grpcCtx := metadata.AppendToOutgoingContext(context.Background(), "X-Hooks-ID", hooksContextID)
		successGrpcReqSent <- struct{}{}
		_, err := kvClient.Get(grpcCtx, &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            testKey,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assert.NoError(s.T(), err, "expected grpc request that starts before shutdown to complete successfully")
	}()

	// Wait for the slow running request to be sent before starting shutdown
	<-eofDapiReqSent
	<-eofGrpcReqSent
	<-successGrpcReqSent
	<-successDapiReqSent

	// Allow the server to start processing the requests
	time.Sleep(time.Second * 1)

	gw.Shutdown()

	wg.Wait()
}
